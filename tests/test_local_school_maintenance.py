from scripts import local_school_maintenance


def test_default_repair_command_uses_local_codex_helper():
    command = local_school_maintenance.default_repair_command()
    assert command == "bash scripts/local_codex_repair.sh {prompt_path}"


def test_build_commands_with_repair_and_deploy():
    commands = local_school_maintenance.build_commands(
        workers=6,
        proxy_profile="datacenter",
        dry_run=False,
        repair=True,
        repair_command=None,
        deploy_ha1=True,
    )

    assert commands == [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            "6",
            "--proxy-profile",
            "datacenter",
        ],
        [
            "uv",
            "run",
            "python",
            "scripts/run_repair_queue.py",
            "--drain-until-empty",
            "--repair-command",
            "bash scripts/local_codex_repair.sh {prompt_path}",
            "--proxy-profile",
            "datacenter",
        ],
        ["uv", "run", "python", "scripts/build_site.py"],
        ["bash", "scripts/deploy-ha1.sh", "--skip-build"],
    ]


def test_build_commands_dry_run_skips_repair_and_deploy():
    commands = local_school_maintenance.build_commands(
        workers=3,
        proxy_profile="mobile",
        dry_run=True,
        repair=True,
        repair_command=None,
        deploy_ha1=True,
    )

    assert commands == [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            "3",
            "--proxy-profile",
            "mobile",
            "--dry-run",
        ],
    ]
