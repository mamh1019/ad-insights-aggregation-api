#!/usr/bin/env python
import bootstrap as _
import typer
import traceback
import pandas as pd

from datetime import datetime, timedelta
from typing import List, Dict, Type, Optional, Tuple
from enum import Enum
from libs.ads.advertisers.advertiser import Advertiser as AdvertiserBase
from libs.ads.publishers.publisher import Publisher as PublisherBase
from libs.ads.mediations.mediation import Mediation as MediationBase

# utils
from libs.utils.date import norm_date
from libs.utils.string import expand_list
from libs.utils.cli import print_table, log
from libs.utils.discovery import discover_subclasses

# scan targets
import libs.ads.advertisers.networks as adv_pkg
import libs.ads.publishers.networks as pub_pkg
import libs.ads.mediations.networks as med_pkg

app = typer.Typer(
    add_completion=False, help="Ads Controller CLI (advertiser/publisher)"
)

# ------------------------------------------------------------------------------
# Global EXCLUDES (temporarily disabled providers)
# Configure via environment variables or settings if needed
# ------------------------------------------------------------------------------
# Example: ADVERTISERS_EXCLUDE = {"Mistplay"}
ADVERTISERS_EXCLUDE = {}
# Place slow or unstable networks at the end (they will run last)
ADVERTISERS_TAIL = {}

PUBLISHERS_EXCLUDE = {}
PUBLISHERS_TAIL = {}

MEDIATIONS_EXCLUDE = {}
MEDIATIONS_TAIL = {}

# ------------------------------------------------------------------------------
# Discover (module scan)
# ------------------------------------------------------------------------------
_ALL_ADVERTISERS: Dict[str, Type[AdvertiserBase]] = discover_subclasses(
    adv_pkg, AdvertiserBase
)
_ALL_PUBLISHERS: Dict[str, Type[PublisherBase]] = discover_subclasses(
    pub_pkg, PublisherBase
)
_ALL_MEDIATIONS: Dict[str, Type[MediationBase]] = discover_subclasses(
    med_pkg, MediationBase
)


def _list_advertisers() -> List[str]:
    tail_set = {x.lower() for x in ADVERTISERS_TAIL}
    names = [n for n in _ALL_ADVERTISERS.keys() if n not in ADVERTISERS_EXCLUDE]
    return sorted(names, key=lambda n: (n.lower() in tail_set, n.lower()))


def _list_publishers() -> List[str]:
    tail_set = {x.lower() for x in PUBLISHERS_TAIL}
    names = [n for n in _ALL_PUBLISHERS.keys() if n not in PUBLISHERS_EXCLUDE]
    return sorted(names, key=lambda n: (n.lower() in tail_set, n.lower()))


def _list_mediations() -> List[str]:
    tail_set = {x.lower() for x in MEDIATIONS_TAIL}
    names = [n for n in _ALL_MEDIATIONS.keys() if n not in MEDIATIONS_EXCLUDE]
    return sorted(names, key=lambda n: (n.lower() in tail_set, n.lower()))


def _create_advertiser(name: str) -> AdvertiserBase:
    return _ALL_ADVERTISERS[name]()


def _create_publisher(name: str) -> PublisherBase:
    return _ALL_PUBLISHERS[name]()


def _create_mediation(name: str) -> MediationBase:
    return _ALL_MEDIATIONS[name]()


# ------------------------------------------------------------------------------
# Interval helpers
# ------------------------------------------------------------------------------
def compute_default_interval() -> Tuple[str, str]:
    """
    Default interval by cron time
    """
    cur_date = pd.to_datetime((datetime.now() - timedelta(hours=0)))
    cur_hour = int(cur_date.hour)

    if cur_hour == 2:
        end = datetime.today() - timedelta(2)
    elif cur_hour == 10:
        end = datetime.today() - timedelta(4)
    elif cur_hour == 14:
        end = datetime.today() - timedelta(6)
    elif cur_hour == 18:
        end = datetime.today() - timedelta(2)  # used by creatives
    else:
        end = datetime.today()
    start = end - timedelta(days=1)

    return (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))


def _resolve_interval(
    start_opt: Optional[str], end_opt: Optional[str]
) -> Tuple[str, str]:
    """
    Apply default rules when start/end unspecified.
    If only one given, adjust to 1-day window.
    """
    s = norm_date(start_opt)  # Assumes Optional[str] return
    e = norm_date(end_opt)

    if s is None and e is None:
        return compute_default_interval()

    if s is None:
        # One day before end
        end_dt = datetime.strptime(e, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=1)
        return start_dt.strftime("%Y-%m-%d"), e

    if e is None:
        # One day after start
        start_dt = datetime.strptime(s, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=1)
        return s, end_dt.strftime("%Y-%m-%d")

    return s, e


# ------------------------------------------------------------------------------
# Advertiser
# ------------------------------------------------------------------------------
adv_app = typer.Typer(help="Advertiser commands")
app.add_typer(adv_app, name="advertiser")


@adv_app.command("list")
def advertiser_list():
    names = _list_advertisers()
    print_table("Advertisers (EXCLUDE applied)", names, "Advertiser")
    if ADVERTISERS_EXCLUDE:
        print_table("Excluded (global)", sorted(ADVERTISERS_EXCLUDE), "Advertiser")


@adv_app.command("cost")
def advertiser_cost(
    provider: List[str] = typer.Option(None, "--provider", "-p"),
    exclude: List[str] = typer.Option(None, "--exclude", "-x"),
    start: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    end: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    list_only: bool = typer.Option(False, "--list"),
):
    """
    Ad cost report
    """
    s, e = _resolve_interval(start, end)
    names = _list_advertisers()

    inc = expand_list(provider)
    exc = set(x.lower() for x in expand_list(exclude))
    if inc:
        names = [n for n in names if n in inc]
    names = [n for n in names if n.lower() not in exc]

    if not names:
        log("[red]No targets[/red]")
        raise typer.Exit(1)

    print_table(f"Cost targets {s} ~ {e}", names, "Advertiser")
    if list_only:
        raise typer.Exit()

    for n in names:
        try:
            adv = _create_advertiser(n)
            if hasattr(adv, "set_interval"):
                adv.set_interval(s, e)
            log(f"[cyan]→ {n}[/cyan] cost interval {s} ~ {e} is running")
            adv.report_cost()
            log(f"[green]✓ {n}[/green] cost interval {s} ~ {e} is completed")
        except Exception as ex:
            message = f"[red]✗ {n} failed[/red] {ex}\n{traceback.format_exc()}"
            log(message)


@adv_app.command("creatives")
def advertiser_creatives(
    provider: List[str] = typer.Option(None, "--provider", "-p"),
    exclude: List[str] = typer.Option(None, "--exclude", "-x"),
    start: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    end: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    list_only: bool = typer.Option(False, "--list"),
):
    s, e = _resolve_interval(start, end)
    names = _list_advertisers()
    inc = expand_list(provider)
    exc = set(x.lower() for x in expand_list(exclude))
    if inc:
        names = [n for n in names if n in inc]
    names = [n for n in names if n.lower() not in exc]

    if not names:
        log("[red]No targets[/red]")
        raise typer.Exit(1)

    print_table(f"Creatives targets {s} ~ {e}", names, "Advertiser")
    if list_only:
        raise typer.Exit()

    for n in names:
        try:
            adv = _create_advertiser(n)
            if hasattr(adv, "set_interval"):
                adv.set_interval(s, e)
            if hasattr(adv, "report_creative"):
                log(f"[bold]→ {n}[/bold] creatives")
                adv.report_creative()
            elif hasattr(adv, "creative"):
                log(f"[bold]→ {n}[/bold] creatives")
                adv.creative()
            else:
                log(f"[yellow]{n} does not support creatives[/yellow]")
        except Exception as ex:
            message = f"[red]✗ {n} failed[/red] {ex}\n{traceback.format_exc()}"
            log(message)


# ------------------------------------------------------------------------------
# Publisher
# ------------------------------------------------------------------------------
pub_app = typer.Typer(help="Publisher commands")
app.add_typer(pub_app, name="publisher")


class PublisherMode(str, Enum):
    PLATFORM = "platform"
    COUNTRY = "country"


@pub_app.command("list")
def publisher_list():
    names = _list_publishers()
    print_table("Publishers (EXCLUDE applied)", names, "Publisher")
    if PUBLISHERS_EXCLUDE:
        print_table("Excluded (global)", sorted(PUBLISHERS_EXCLUDE), "Publisher")


def _run_publishers(
    mode: PublisherMode,
    provider: List[str],
    exclude: List[str],
    start: Optional[str],
    end: Optional[str],
    list_only: bool,
):
    s, e = _resolve_interval(start, end)
    names = _list_publishers()

    inc = expand_list(provider)
    exc = set(x.lower() for x in expand_list(exclude))
    if inc:
        names = [n for n in names if n in inc]
    names = [n for n in names if n.lower() not in exc]

    if not names:
        log("[red]No targets[/red]")
        raise typer.Exit(1)

    title = "Platform" if mode == PublisherMode.PLATFORM else "Country"
    print_table(f"{title} targets {s} ~ {e}", names, "Publisher")
    if list_only:
        raise typer.Exit()

    for n in names:
        try:
            pub = _create_publisher(n)
            if hasattr(pub, "set_interval"):
                pub.set_interval(s, e)
            log(f"[cyan]→ {n}[/cyan] publisher report interval {s} ~ {e} is running")
            if mode == PublisherMode.PLATFORM and hasattr(pub, "report_platform"):
                pub.report_platform()
            elif mode == PublisherMode.COUNTRY and hasattr(pub, "report_country"):
                pub.report_country()
            else:
                log(f"[yellow]{n} does not support {mode.value}[/yellow]")
            log(
                f"[green]✓ {n}[/green] publisher report interval {s} ~ {e} is completed"
            )
        except Exception as ex:
            message = f"[red]✗ {n} failed[/red] {ex}\n{traceback.format_exc()}"
            log(message)


@pub_app.command("platform")
def publisher_platform(
    provider: List[str] = typer.Option(None, "--provider", "-p"),
    exclude: List[str] = typer.Option(None, "--exclude", "-x"),
    start: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    end: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    list_only: bool = typer.Option(False, "--list"),
):
    """
    Platform-based revenue report (better consistency than country report)
    """
    _run_publishers(PublisherMode.PLATFORM, provider, exclude, start, end, list_only)


@pub_app.command("country")
def publisher_country(
    provider: List[str] = typer.Option(None, "--provider", "-p"),
    exclude: List[str] = typer.Option(None, "--exclude", "-x"),
    start: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    end: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    list_only: bool = typer.Option(False, "--list"),
):
    """
    Country-based revenue report
    """
    _run_publishers(PublisherMode.COUNTRY, provider, exclude, start, end, list_only)


# ------------------------------------------------------------------------------
# Mediation
# ------------------------------------------------------------------------------
med_app = typer.Typer(help="Mediation commands")
app.add_typer(med_app, name="mediation")


@med_app.command("list")
def mediation_list():
    names = _list_mediations()
    print_table("Mediations (EXCLUDE applied)", names, "Mediation")
    if MEDIATIONS_EXCLUDE:
        print_table("Excluded (global)", sorted(MEDIATIONS_EXCLUDE), "Mediation")


@med_app.command("revenue")
def mediation_revenue(
    provider: List[str] = typer.Option(None, "--provider", "-p"),
    exclude: List[str] = typer.Option(None, "--exclude", "-x"),
    start: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    end: Optional[str] = typer.Option(None, help="YYYY-MM-DD | today"),
    list_only: bool = typer.Option(False, "--list"),
):
    """
    Mediation-based revenue report
    """
    s, e = _resolve_interval(start, end)
    names = _list_mediations()

    inc = expand_list(provider)
    exc = set(x.lower() for x in expand_list(exclude))
    if inc:
        names = [n for n in names if n in inc]
    names = [n for n in names if n.lower() not in exc]

    if not names:
        log("[red]No targets[/red]")
        raise typer.Exit(1)

    print_table(f"Revenue targets {s} ~ {e}", names, "Mediation")
    if list_only:
        raise typer.Exit()

    for n in names:
        try:
            med = _create_mediation(n)
            if hasattr(med, "set_interval"):
                med.set_interval(s, e)
            log(f"[cyan]→ {n}[/cyan] revenue interval {s} ~ {e} is running")
            med.report_revenue()
            log(f"[green]✓ {n}[/green] revenue interval {s} ~ {e} is completed")
        except Exception as ex:
            message = f"[red]✗ {n} failed[/red] {ex}\n{traceback.format_exc()}"
            log(message)


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        app()
    except Exception as ex:
        tb_str = traceback.format_exc()
        log(f"[red]✗ {ex}[/red]\n{tb_str}")
