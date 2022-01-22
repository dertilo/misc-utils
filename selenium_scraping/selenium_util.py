from typing import Generator, Iterator, ContextManager

import os
from contextlib import contextmanager
from time import sleep

from beartype import beartype
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver


@contextmanager
def ChromeDriver(download_dir: str, headless=True, window_size=(1920, 1080)) -> ContextManager[webdriver.Chrome]:  # type: ignore
    """
    Iterator[webdriver.Chrome] might be correct typing but pycharm does not get it so:
    see: https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers ContextManager[webdriver.Chrome]
    """
    os.makedirs(download_dir, exist_ok=True)
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("headless")

    w, h = window_size
    options.add_argument(f"--window-size={w},{h}")
    # driver.execute_script("document.body.style.zoom='80 %'")
    prefs = {
        "download.default_directory": download_dir,
        "plugins.always_open_pdf_externally": True,  # don't open pdfs in browser but instead download them
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(
        r"/usr/bin/chromedriver",
        chrome_options=options,
    )  # provide the chromedriver execution path in case of error
    driver.implicitly_wait(10)  # seconds
    try:
        yield driver
    finally:
        driver.close()


@beartype
def enter_keyboard_input(
    wd: WebDriver, xpath: str, keyboard_input: str, clear_it=False, press_enter=False
):
    # wait = WebDriverWait(wd, 10)
    # wait.until(EC.presence_of_element_located((By.xpath(value), "content")))
    e = wd.find_element(by=By.XPATH, value=xpath)
    if clear_it:
        e.clear()
    e.send_keys(keyboard_input)
    if press_enter:
        e.send_keys(Keys.ENTER)


def retry(
    fun,
    num_retries=3,
    wait_time=1.0,
    increase_wait_time=False,
    do_raise=True,
    default=None,
    fail_message=None,
):
    exception = None
    for k in range(num_retries):
        try:
            return fun()
        except Exception as e:
            exception = e
            print(f"\n{exception=}\n")
            if increase_wait_time:
                waiting_time = wait_time * 2 ** k
            else:
                waiting_time = wait_time
            sleep(waiting_time)
    print(f"retry failed {num_retries} times!")
    if do_raise:
        if fail_message:
            print(fail_message)
        raise exception
    else:
        return default


def click_it(wd, xpath):
    element = wd.find_element_by_xpath(xpath)
    element.click()
