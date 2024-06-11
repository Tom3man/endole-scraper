import random
from threading import Lock
from typing import Optional

import orb.spinner.utils as orb_utils
from orb.common.vpn import PiaVpn
from orb.spinner.core.driver import OrbDriver
from selenium.webdriver.chrome.webdriver import WebDriver

ip_change_lock = Lock()


def manage_browser_settings(
        driver: WebDriver, orb: OrbDriver,
        viewport_change: Optional[int] = 3,
        vpn_change: Optional[int] = 40,
        driver_change: Optional[int] = 20,
) -> WebDriver:
    """
    Randomly changes browser settings to mimic human behavior and avoid detection.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        orb (OrbDriver): Custom driver class instance used to manage settings.
    """
    if random.randint(1, viewport_change) == 2:
        orb_utils.change_viewport_size(driver=driver)

    if random.randint(1, vpn_change) == 2:
        with ip_change_lock:
            PiaVpn().rotate_vpn()
            driver = orb.refresh_driver()

    if random.randint(1, driver_change) == 2:
        driver = orb.refresh_driver()

    return driver
