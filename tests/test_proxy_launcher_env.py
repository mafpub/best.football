from pathlib import Path
from types import SimpleNamespace

from scripts import create_scraper_from_url, run_repair_queue, school_creator_loop


def test_school_creator_loop_uses_selected_proxy_env(monkeypatch):
    captured: dict[str, str] = {}
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-gateway.example")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")

    def fake_run(command, **kwargs):
        captured.update(kwargs["env"])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(school_creator_loop.subprocess, "run", fake_run)

    school_creator_loop._run_creator_command(["echo", "creator"], "datacenter")

    assert captured["OXYLABS_PROXY_PROFILE"] == "datacenter"
    assert captured["HTTPS_PROXY"] == "https://dc-user:dc-pass@dc-gateway.example"


def test_create_scraper_from_url_passes_selected_proxy_env(monkeypatch, tmp_path):
    captured: dict[str, str] = {}
    script_path = tmp_path / "generated.py"
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-gateway.example")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")
    monkeypatch.setattr(create_scraper_from_url.queue, "resolve_script_path", lambda *_: script_path)

    def fake_run(command, **kwargs):
        captured.update(kwargs["env"])
        return SimpleNamespace(
            returncode=0,
            stdout='{"status":"complete","script_path":"' + str(script_path) + '"}',
            stderr="",
        )

    monkeypatch.setattr(create_scraper_from_url.subprocess, "run", fake_run)

    result = create_scraper_from_url._run_adapter(
        {
            "nces_id": "1",
            "name": "Alpha High",
            "state": "CA",
            "website": "https://alpha.example",
            "city": "Denver",
        },
        "launcher --prompt {prompt_path}",
        proxy_profile="datacenter",
    )

    assert result["status"] == "complete"
    assert captured["OXYLABS_PROXY_PROFILE"] == "datacenter"
    assert captured["HTTP_PROXY"] == "https://dc-user:dc-pass@dc-gateway.example"


def test_run_repair_queue_uses_selected_proxy_env(monkeypatch, tmp_path):
    captured: dict[str, str] = {}
    script_path = tmp_path / "repair.py"
    script_path.write_text("# repaired", encoding="utf-8")
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-gateway.example")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")
    monkeypatch.setattr(
        run_repair_queue.queue,
        "claim_next_school",
        lambda **_: {
            "nces_id": "2",
            "name": "Repair High",
            "state": "CA",
            "website": "https://repair.example",
            "city": "Denver",
            "scraper_file": str(script_path),
            "failure_reason": "broken_selectors",
        },
    )
    monkeypatch.setattr(run_repair_queue.queue, "mark_needs_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_repair_queue.queue, "mark_blocked", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_repair_queue.queue, "mark_complete", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_repair_queue, "require_proxy_credentials", lambda profile=None: None)
    monkeypatch.setattr(run_repair_queue, "assert_not_blocklisted", lambda urls, profile=None: None)
    monkeypatch.setattr(
        run_repair_queue,
        "run_scraper_file_sync",
        lambda *args, **kwargs: SimpleNamespace(valid=True, non_empty_extraction=True),
    )

    def fake_run(command, **kwargs):
        captured.update(kwargs["env"])
        return SimpleNamespace(
            returncode=0,
            stdout='{"status":"complete","script_path":"' + str(script_path) + '"}',
            stderr="",
        )

    monkeypatch.setattr(run_repair_queue.subprocess, "run", fake_run)

    handled = run_repair_queue._process_one(
        "launcher --prompt {script_path}",
        state=None,
        dry_run=False,
        proxy_profile="datacenter",
    )

    assert handled is True
    assert captured["OXYLABS_PROXY_PROFILE"] == "datacenter"
    assert captured["ALL_PROXY"] == "https://dc-user:dc-pass@dc-gateway.example"
