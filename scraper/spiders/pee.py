import os
import re
from datetime import datetime, timedelta

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem


TARGETS = [
    # Grand Est
    {"authority": "Préfet", "region": "Grand Est"},
    {"authority": "MRAe", "region": "Grand Est"},
    # Bourgogne-Franche-Comté
    {"authority": "Préfet", "region": "Bourgogne-Franche-Comté"},
    {"authority": "MRAe", "region": "Bourgogne-Franche-Comté"},
    # Pays de la Loire
    {"authority": "Préfet", "region": "Pays de la Loire"},
    {"authority": "MRAe", "region": "Pays de la Loire"},
    # Provence-Alpes-Côte d'Azur
    {"authority": "Préfet", "region": "Provence-Alpes-Côte d'Azur"},
    {"authority": "MRAe", "region": "Provence-Alpes-Côte d'Azur"},
]


RESULTS_LIST_API_URL = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get?start={start}&length={length}&descending_order_id=true&authority={authority}&place={region}"

RESULTS_LENGTH = 100

PROJECT_PAGE_API_URL = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/GetByDocumentId?documentId={document_id}"

PROJECT_PAGE_WEB_URL = "https://evaluation-environnementale.developpement-durable.gouv.fr/#/public/view-document/{document_id}"

DOCUMENT_DOWNLOAD_URL = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/Attachment/PublishedDownload?ctsFileId={file_id}"


class PEESpider(scrapy.Spider):
    name = "PEE_spider"

    upload_limit_attained = False

    start_time = datetime.now()

    def check_time_limit(self):
        """Closes the spider automatically if it reaches a specified duration"""

        # self.logger.info(f"Checking time limit ({self.time_limit} min)")

        if self.time_limit != 0:

            limit = self.time_limit * 60
            now = datetime.now()

            if timedelta.total_seconds(now - self.start_time) > limit:
                raise CloseSpider(
                    f"Closed due to time limit ({self.time_limit} minutes)"
                )

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def start_requests(self):

        requests = []

        for target in TARGETS:

            url = RESULTS_LIST_API_URL.format(
                start=0,
                length=RESULTS_LENGTH,
                authority=target["authority"],
                region=target["region"],
            )

            requests.append(
                scrapy.Request(
                    url,
                    cb_kwargs=dict(
                        authority=target["authority"], region=target["region"], page=1
                    ),
                    callback=self.parse_results,
                )
            )

        return requests

    def parse_results(self, response, authority, region, page):

        self.check_upload_limit()
        self.check_time_limit()

        data = response.json()
        total = data["totalCount"]
        self.logger.info(f"Parsing {authority}/{region}, page {page}")

        # Yield a request per entry/project
        for project in data["data"]:

            project_created_year = int(project["publishedDate"][:4])

            if project_created_year in self.target_years:

                doc_id = project["documentId"]
                url = PROJECT_PAGE_API_URL.format(document_id=doc_id)

                # Check if some file_ids are not in event_data

                if project["publishedAttachmentIds"] == "":
                    continue

                file_ids = [
                    int(x) for x in project["publishedAttachmentIds"].split(",")
                ]

                already_fully_scraped = True
                for f_id in file_ids:
                    if (
                        not DOCUMENT_DOWNLOAD_URL.format(file_id=f_id)
                        in self.event_data
                    ):
                        already_fully_scraped = False

                if not already_fully_scraped:
                    yield scrapy.Request(
                        url,
                        callback=self.parse_project_page,
                        cb_kwargs=dict(document_id=doc_id),
                    )

        # Next page
        if page * RESULTS_LENGTH < total:

            next_page_url = RESULTS_LIST_API_URL.format(
                start=page * RESULTS_LENGTH,
                length=RESULTS_LENGTH,
                authority=authority,
                region=region,
            )
            yield scrapy.Request(
                next_page_url,
                cb_kwargs=dict(authority=authority, region=region, page=page + 1),
                callback=self.parse_results,
            )

    def parse_project_page(self, response, document_id):

        self.check_upload_limit()
        self.check_time_limit()

        data = response.json()

        # Project name
        municipality = data["municipality"]
        project_title = data["projectTitle"].strip(" -.")

        if municipality.lower() not in project_title.lower():
            project_title += " - " + municipality

        for a in data["attachments"]:

            file_id = a["id"]

            # Check event_data
            if not DOCUMENT_DOWNLOAD_URL.format(file_id=file_id) in self.event_data:

                # Publication Date
                if a["folderName"] in ["Décision", "Avis"]:
                    if data["updatedDate"]:
                        publication_timestamp = data["updatedDate"]
                    else:
                        publication_timestamp = data["publishedDate"]
                else:
                    publication_timestamp = data["publishedDate"]

                # Add "Décision" to the title if not present
                doc_title = a["name"]
                if a["folderName"] == "Décision":
                    if (
                        not "décision" in doc_title.lower()
                        and not "decision" in doc_title.lower()
                    ):
                        doc_title = a["folderName"] + " - " + doc_title.strip()

                doc_item = DocumentItem(
                    # title=a["folderName"] + " - " + a["name"],
                    title=doc_title,
                    project=project_title,
                    authority=data["authority"],
                    category_local=data["categoryName"],
                    source_file_url=DOCUMENT_DOWNLOAD_URL.format(file_id=file_id),
                    source_page_url=PROJECT_PAGE_WEB_URL.format(
                        document_id=document_id
                    ),
                    source_filename=a["name"] + "." + a["extension"],
                    publication_timestamp=publication_timestamp,
                    year=data["publishedDate"][:4],
                )

                yield scrapy.Request(
                    doc_item["source_file_url"],
                    callback=self.download_document,
                    cb_kwargs=dict(doc_item=doc_item, file_id=file_id),
                )

    def download_document(self, response, doc_item, file_id):

        self.check_upload_limit()
        self.check_time_limit()

        # Create the folder to hold all files if it does not exist yet
        if not os.path.exists("./downloaded_files"):
            os.makedirs("./downloaded_files")

        # Create a folder to hold the current file if it does not exist yet
        if not os.path.exists(f"./downloaded_files/{file_id}"):
            os.makedirs(f"./downloaded_files/{file_id}")

        # Save the file in the folder

        local_file_path = f"./downloaded_files/{file_id}/{doc_item['source_filename']}"
        with open(local_file_path, "wb") as file:
            file.write(response.body)

        doc_item["local_file_path"] = local_file_path

        yield doc_item
