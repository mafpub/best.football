from scripts import weekly_scrape


def test_build_weekly_commands_without_repair_command():
    commands = weekly_scrape.build_weekly_commands(
        dry_run=False,
        repair_command=None,
        workers=8,
        proxy_profile="datacenter",
    )

    assert commands == [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            "8",
            "--proxy-profile",
            "datacenter",
        ],
        ["uv", "run", "python", "scripts/build_site.py"],
    ]


def test_build_weekly_commands_with_repair_command():
    commands = weekly_scrape.build_weekly_commands(
        dry_run=False,
        repair_command="codex repair --prompt {prompt_path}",
        workers=4,
        proxy_profile="datacenter",
    )

    assert commands == [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            "4",
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
            "codex repair --prompt {prompt_path}",
            "--proxy-profile",
            "datacenter",
        ],
        ["uv", "run", "python", "scripts/build_site.py"],
    ]


def test_build_weekly_commands_dry_run_skips_build_and_repair():
    commands = weekly_scrape.build_weekly_commands(
        dry_run=True,
        repair_command="codex repair --prompt {prompt_path}",
        workers=2,
        proxy_profile="mobile",
    )

    assert commands == [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            "2",
            "--proxy-profile",
            "mobile",
            "--dry-run",
        ],
    ]
