"""CLI for inspecting, serving, registering, and deploying UDF bundles."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .bundle import build_manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rw-udf")
    commands = parser.add_subparsers(dest="command", required=True)

    manifest = commands.add_parser(
        "manifest", help="Print the deployable function manifest"
    )
    manifest.add_argument("--module", required=True)
    manifest.add_argument("--project-root", type=Path, default=Path.cwd())

    server = commands.add_parser("serve", help="Serve a bundle over Arrow Flight")
    server.add_argument("--module", required=True)
    server.add_argument("--host", default="0.0.0.0")
    server.add_argument("--port", type=int, default=8815)
    server.add_argument("--project-root", type=Path, default=Path.cwd())

    validate = commands.add_parser(
        "validate",
        help="Validate a deployed bundle over Arrow Flight",
    )
    validate.add_argument("--module", required=True)
    validate.add_argument("--udf-url", required=True)
    validate.add_argument("--timeout", type=float, default=5)
    validate.add_argument("--allow-extra-functions", action="store_true")
    validate.add_argument("--project-root", type=Path, default=Path.cwd())

    register = commands.add_parser(
        "register",
        help="Register a deployed bundle in RisingWave",
    )
    register.add_argument("--module", required=True)
    register.add_argument(
        "--dsn",
        required=True,
        help="risingwave:// or postgresql:// connection URL",
    )
    register.add_argument("--udf-url", required=True)
    register.add_argument("--project-root", type=Path, default=Path.cwd())

    deploy = commands.add_parser(
        "deploy",
        help="Deploy a bundle to AWS ECS Fargate",
    )
    deploy.add_argument("--module", required=True)
    deploy.add_argument(
        "--target",
        choices=("aws-fargate",),
        default="aws-fargate",
    )
    deploy.add_argument("--name", required=True)
    deploy.add_argument("--region", required=True)
    deploy.add_argument("--allowed-principal", action="append", required=True)
    deploy.add_argument("--aws-profile")
    deploy.add_argument("--project-root", type=Path, default=Path.cwd())
    deploy.add_argument("--desired-count", type=int, default=2)
    deploy.add_argument("--cpu", type=int, default=1024)
    deploy.add_argument("--memory", type=int, default=2048)
    deploy.add_argument("--port", type=int, default=8815)
    deploy.add_argument(
        "--image-uri",
        help="Use an existing digest-pinned image and skip Docker/ECR",
    )
    deploy.add_argument(
        "--extra",
        action="append",
        default=[],
        help="Python project extra to install",
    )
    deploy.add_argument(
        "--include",
        action="append",
        default=[],
        help="Additional project-relative path to include in the image",
    )
    deploy.add_argument("--state-file", type=Path)

    rollback = commands.add_parser(
        "rollback",
        help="Roll an AWS Fargate service back to a recorded image digest",
    )
    rollback.add_argument("--name", required=True)
    rollback.add_argument("--version", required=True)
    rollback.add_argument("--aws-profile")
    rollback.add_argument("--project-root", type=Path, default=Path.cwd())
    rollback.add_argument("--state-file", type=Path)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    project_root = args.project_root.resolve()
    sys.path.insert(0, str(project_root))
    if args.command == "manifest":
        print(build_manifest(args.module).to_json(), end="")
        return
    if args.command == "serve":
        from .runtime import serve

        serve(args.module, host=args.host, port=args.port)
        return
    if args.command == "validate":
        from .health import validate_flight_manifest

        functions = validate_flight_manifest(
            args.udf_url,
            build_manifest(args.module),
            timeout=args.timeout,
            allow_extra_functions=args.allow_extra_functions,
        )
        print(json.dumps({"ready": True, "functions": functions}, indent=2))
        return
    if args.command == "register":
        from risingwave import RisingWave, RisingWaveConnOptions

        with RisingWave(RisingWaveConnOptions(args.dsn)) as risingwave:
            statements = risingwave.udf.register_bundle(
                args.module,
                udf_url=args.udf_url,
            )
        print(
            json.dumps(
                {
                    "registered": len(statements),
                    "statements": statements,
                },
                indent=2,
            )
        )
        return

    from .deploy.aws import AwsFargateDeployer, FargateConfig
    from .deploy.state import DeploymentStateStore, manifests_are_compatible

    if args.command == "rollback":
        state_file = (
            args.state_file or Path(".rw-udf/deployments") / f"{args.name}.json"
        )
        store = DeploymentStateStore(state_file, name=args.name)
        history = store.load()
        latest = history.latest
        if latest is None:
            raise RuntimeError(f"no deployment history found for {args.name!r}")
        target = history.find(args.version)
        if target.deployment_id == latest.deployment_id:
            raise RuntimeError(f"deployment {target.deployment_id!r} is already active")
        if not manifests_are_compatible(latest.manifest, target.manifest):
            raise RuntimeError(
                "rollback would change SQL-visible function signatures; "
                "run an explicit SQL migration instead"
            )
        recorded = target.config
        config = FargateConfig(
            name=recorded.name,
            module=recorded.module,
            project_root=project_root,
            region=recorded.region,
            allowed_principals=recorded.allowed_principals,
            aws_profile=args.aws_profile,
            desired_count=recorded.desired_count,
            cpu=recorded.cpu,
            memory=recorded.memory,
            port=recorded.port,
            image_uri=target.image_uri,
            extras=recorded.extras,
            build_includes=recorded.build_includes,
            uv_version=recorded.uv_version,
            uv_image=recorded.uv_image,
            python_image=recorded.python_image,
        )
        result = AwsFargateDeployer(config).deploy(
            target.manifest,
            rollback_of=target.deployment_id,
            source=target,
        )
        store.append(result)
        print(result.to_json(), end="")
        return

    manifest = build_manifest(args.module)
    config = FargateConfig(
        name=args.name,
        module=args.module,
        project_root=project_root,
        region=args.region,
        allowed_principals=tuple(args.allowed_principal),
        aws_profile=args.aws_profile,
        desired_count=args.desired_count,
        cpu=args.cpu,
        memory=args.memory,
        port=args.port,
        image_uri=args.image_uri,
        extras=tuple(args.extra),
        build_includes=tuple(args.include),
    )
    result = AwsFargateDeployer(config).deploy(manifest)
    state_file = args.state_file or Path(".rw-udf/deployments") / f"{args.name}.json"
    DeploymentStateStore(state_file, name=args.name).append(result)
    print(result.to_json(), end="")


if __name__ == "__main__":
    main()
