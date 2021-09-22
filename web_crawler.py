# import packages
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

# Build Chrome webdriver
driver_path = "./chromedriver_win32/chromedriver.exe"
driver = webdriver.Chrome(executable_path=driver_path)

# Connect to the website
driver.get("https://plvr.land.moi.gov.tw/DownloadOpenData")

# Close the other popup website
parent = driver.window_handles[0]
child = driver.window_handles[1]
driver.switch_to.window(child)
driver.close()
driver.switch_to.window(parent)

## 點擊按鈕
# 非本期下載
button1 = driver.find_element_by_id("ui-id-2") 
button1.click()

# 發布日期:108年第2季
driver.set_page_load_timeout(20)
tools = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='historySeason_id']")))
select_season = Select(tools)
select_season.select_by_value("108S2")

# 下載檔案格式: csv
file_format_bt = driver.find_element_by_xpath("//*[@id='fileFormatId']/option[3]") # 下載檔案格式: csv
file_format_bt.click()

# 下載方式: 進階下載
advance_download_bt = driver.find_element_by_id("downloadTypeId2") 
advance_download_bt.click()

# 不動產買賣:台北市、新北市、桃園市、台中市、高雄市
city_values = ['A_lvr_land_A','B_lvr_land_A','E_lvr_land_A','F_lvr_land_A','H_lvr_land_A'] 
for value in city_values:
    css_path = "input[value={}]".format(value)
    city_bt = driver.find_element_by_css_selector(css_path)
    city_bt.click()

# 下載紐
download_bt = driver.find_element_by_id("downloadBtnId") 
download_bt.click()

