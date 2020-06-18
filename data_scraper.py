import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import re
from bs4 import BeautifulSoup
from tzlocal import get_localzone
from datetime import datetime, timezone

DEFAULT_TIMEOUT = 15

retry_param = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
)

http = requests.Session()
adapter = HTTPAdapter(max_retries=retry_param)
http.mount("https://", adapter)
http.mount("http://", adapter)


class data_scr():
    def __init__(self, data_url):
        self.host_url = data_url
        self.inti_dt = datetime.now(get_localzone())
        #self.data = {}
        #self.open_data = json.loads(json.dumps(self.data))

    def request_data(self, url, fp1, fp2, fp3):
        page = http.get(url, timeout=DEFAULT_TIMEOUT)
        soup = BeautifulSoup(page.content, "html.parser")
        contents = soup.find_all(fp1, {fp2: fp3})
        return soup, contents

    def find_subj_page(self,subj_names): # find all/specified subjects and their data from given web address https://open.canada.ca/en/open-data
        subj_list = []
        subj_dict = {}
        subj_no = 0
        host_page, host_data = self.request_data(self.host_url, 'ul', 'class', 'subjects')
        for host_subject in host_data:
            host_category = host_subject.find_all("a", href=True)
            for host_cate_data in host_category:
                if subj_names.replace(" ","").upper() == "ALL":
                   subj_names =  host_cate_data.find("span", {"class": "small"}).text
                if host_cate_data.find("span", {"class": "small"}).text == subj_names.strip():
                    Print ("Found the specified subject, continue to analyzing it's record sets")
                    subj_list.append({})
                    subj_dict = {
                        "Subj_name": host_cate_data.find("span", {"class": "small"}).text,
                        "Subj_url": host_cate_data['href'], "Subj_pages": self.find_subj_all_pages(host_cate_data['href']) #Search all of the records under this subject
                    }
                    subj_list[subj_no] = subj_dict
                    subj_no += 1
                    print(host_cate_data.find("span", {"class": "small"}).text)
                else:
                    print ("No Subject is specified, try next...")
                    continue
        if subj_no == 0:
            print ("Did not find the inputed subject, existing")        
        return subj_list

    def find_subj_all_pages(self, subj_url): # This fuction generate all web pages (search feature) address under the specific subject, then pass each address over to find_record_page function to scan each individual pages
        urls = []
        page_list = []
        cate_pages, cate_page_data = self.request_data(subj_url, 'div', 'class', 'col-md-4 col-md-pull-8')
        page_meta_main = cate_pages.findAll("ul", {"class": "pagination"})
        for page_lists in page_meta_main:
            page_list_temp = page_lists.find_all("a", {"href": "#"})
            for page_nums in page_list_temp:
                if page_nums.text.split('\n')[0].isdigit():
                    total_page_num = page_nums.text.split('\n')[0]

        for page_next_info in cate_page_data:
            for num in range(1, int(total_page_num) + 1):
                page_list.append({})
                page_num = num
                urls.append(
                    'https://search.open.canada.ca' + page_next_info.find("a", {"onclick": ""})['href'].replace('amp;',                                                                                                                '').replace(
                     'export/', '').replace('+', '%20').replace('?', '?sort=score%20desc&page=' + str(num) + '&search_text=&'))
                page_url = urls[num - 1]
                print(page_url)
                page_dict = {
                    "Page_num": page_num, "Page_url": page_url,
                    "Page_records": self.find_record_page(page_url)
                }
                page_list[num - 1] = page_dict
        return page_list

    def find_record_file(self, record_page_url): # Prepare data for each file under every records
        record_file_soup, record_data = self.request_data(record_page_url, 'div', 'class', 'panel panel-primary')
        rec_id = ''
        for important_cont in record_data:
            if important_cont.find('strong', text='Record ID:'):
                rec_id = important_cont.find('strong', text='Record ID:').nextSibling.strip()
        print(rec_id)

        file_list = []
        file_no = 0
        file_dict = {}
        file_data = record_file_soup.findAll('div', {'vocab': 'http://schema.org/'})
        for file_info in file_data:
            record_publisher = file_info.find('span',property='name')['value']
            datadownload = file_info.findAll('span', property='distribution', typeof='DataDownload')
            for data_link in datadownload:
                data1 = data_link.find_all('span', property="inLanguage", content="en")
                if data1:
                    file_list.append({})
                    file_dict = {'File_name': data1[0].parent.find('span', property='name').text,
                                 'File_format': data1[0].parent.find('span', property='fileFormat').text,
                                 'File_language': 'EN',
                                 'File_url': data1[0].parent.find('span', property='url').text,
                                 'Downloaded': 'N',
                                 'Saved_path': ''}
                    file_list[file_no] = file_dict
                    file_no += 1

                data2 = data_link.find_all('span', property='inLanguage', content='en,fr')
                if data2:
                    file_list.append({})
                    file_dict = {'File_name': data2[0].parent.find('span', property='name').text.strip(),
                                 'File_format': data2[0].parent.find('span', property='fileFormat').text.strip(),
                                 'File_language': 'EN',
                                 'File_url': data2[0].parent.find('span', property='url').text.strip(),
                                 'Downloaded': 'N',
                                 'Saved_path': ''}
                    file_list[file_no] = file_dict
                    file_no += 1

            # print (file_list)
        return rec_id, file_list

    def find_record_page(self, categ_list_url):  # Prepare data for each record
        record_page_soup, record_page_data = self.request_data(categ_list_url, 'div', 'class',
                                                               'panel panel-default mrg-tp-sm')
        if record_page_data:
            record_list = []
            record_no = 0
            for urls in record_page_data:
                record_list.append({})
                record_name = ''
                record_url = ''
                record_id = ''
                date_published = ''
                date_last_updated = ''
                record_description = ''

                #record_name = re.sub(r'[\r\n]+', '', urls.find('a').text.strip())
                record_name = urls.find('a').text.strip()
                print(record_name)
                record_url = urls.find('a', href=True)['href'].strip()
                print(record_url)
                record_id, file = self.find_record_file(record_url)

                if urls.find('strong', text='Date Published: '):
                    date_published = urls.find('strong', text='Date Published: ').nextSibling.strip()

                if urls.find('strong', text='Last Updated: '):
                    date_last_updated = urls.find('strong', text='Last Updated: ').nextSibling.strip()

                # Record_description = re.sub(r'[\r\n]+', '', urls.find('p').text.strip())
                if urls.find('p'):
                    record_description = urls.find('p').text.strip()

                record_dict = {"Record_ID": record_id, "Record_name": record_name,
                               "Record_description": record_description, "Date_published": date_published,
                               "Date_last_updated": date_last_updated, "Record_URL": record_url, "Record_files": file}
                record_list[record_no] = record_dict
                record_no += 1
            # print (record_list)
        return record_list

if __name__ == '__main__':

    open_data ={}
    data = json.loads(json.dumps(open_data))


    temp_data = data_scr('https://open.canada.ca/en/open-data')

    data = {}
    data = {
        "Create_datetime": datetime.now().strftime("%A, %d. %B %Y %I:%M%p"),
        
        #"Subjects": temp_data.find_subj_page("all")    # this is where you specify what subject you want to download, input "all" will download all of the subjects from website https://open.canada.ca/en/open-data:
        #"Subjects": temp_data.find_subj_page("Economics and I") #if you input an incorrect subject name like this, scraper outputs "Did not find the inputed subject, existing" then wrtie an empty subject list into open_data.txt  
        "Subjects": temp_data.find_subj_page("Economics and Industry")  # if you input a correct subject name, then only the data under this subject will be scraped
        }

    open_data = json.dumps(data, ensure_ascii=False)

    with open('open_data.txt', 'w') as outfile:
        json.dump(open_data, outfile)
