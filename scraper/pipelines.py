# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json
import hashlib
import shutil

from itemadapter import ItemAdapter

from scrapy.exceptions import DropItem

from documentcloud.constants import SUPPORTED_EXTENSIONS

from .log import SilentDropItem
from .departments import department_from_authority, departments_from_project_name


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item, spider):
        """Parses date from the extracted string."""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_timestamp"], "%Y-%m-%dT%H:%M:%S.%f"
        )

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")

        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        item["publication_datetime_dcformat"] = (
            publication_dt.isoformat(timespec="microseconds") + "Z"
        )

        return item


class UnsupportedFiletypePipeline:

    def process_item(self, item, spider):

        filename, file_extension = os.path.splitext(item["source_filename"])
        file_extension = file_extension.lower()

        if file_extension not in SUPPORTED_EXTENSIONS:
            # Drop the item
            raise DropItem("Unsupported filetype")
        else:
            return item


class BeautifyPipeline:
    def process_item(self, item, spider):
        """Beautify & harmonize project & title names."""

        # Project
        item["project"] = item["project"].strip()
        item["project"] = item["project"].replace(" ", " ").replace("’", "'")
        item["project"] = item["project"].rstrip(".,")

        item["project"] = item["project"][0].capitalize() + item["project"][1:]

        # Title
        item["title"] = item["title"].replace("_", " ")
        item["title"] = item["title"].rstrip(".,")
        item["title"] = item["title"].strip("-")
        item["title"] = item["title"].strip()

        item["title"] = item["title"][0].capitalize() + item["title"][1:]

        # Authority

        item["authority"] = item["authority"].replace(
            "Préfet de la région", "Préfecture de région"
        )

        item["authority"] = item["authority"].replace("MRae de la région", "MRAe")

        item["authority"] = item["authority"].replace(
            "Autorité Environnementale Ministre (CGDD)", "Ministère de l'Environnement"
        )

        # Category
        item["category_local"] = (
            item["category_local"].replace(" ", " ").replace("’", "'")
        )

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item, spider):

        if "cas par cas" in item["category_local"].lower():
            item["category"] = "Cas par cas"

        elif item["category_local"].startswith("Demande d'avis"):
            item["category"] = "Avis"

        return item


class UploadLimitPipeline:
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self, spider):
        self.number_of_docs = 0

    def process_item(self, item, spider):
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class TagDepartmentsPipeline:

    def process_item(self, item, spider):

        authority_department = department_from_authority(item["authority"])

        if authority_department:
            item["departments_sources"] = ["authority"]
            item["departments"] = [authority_department]

        else:

            project_departments = departments_from_project_name(item["project"])

            if project_departments:

                item["departments_sources"] = ["regex"]
                item["departments"] = project_departments

        return item


class ProjectIDPipeline:

    def process_item(self, item, spider):

        project_name = item["project"]
        source_page_url = item["source_page_url"]
        string_to_hash = source_page_url + " " + project_name

        hash_object = hashlib.sha256(string_to_hash.encode())
        hex_dig = hash_object.hexdigest()

        item["project_id"] = hex_dig

        return item


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self, spider):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)

        if not spider.dry_run:
            try:
                spider.logger.info("Loading event data from DocumentCloud...")
                spider.event_data = spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            # Load from json if present
            try:

                with open("event_data.json", "r") as file:
                    spider.logger.info("Loading event data from local JSON file...")
                    data = json.load(file)
                    spider.event_data = data
            except:
                spider.event_data = None

        if spider.event_data:
            spider.logger.info(
                f"Loaded event data ({len(spider.event_data)} documents)"
            )
        else:
            spider.logger.info("No event data was loaded.")
            spider.event_data = {}

    def process_item(self, item, spider):

        data = {
            "authority": item["authority"],
            "category": item["category"],
            "category_local": item["category_local"],
            "event_data_key": item["source_file_url"],
            "publication_date": item["publication_date"],
            "publication_time": item["publication_time"],
            "publication_datetime": item["publication_datetime"],
            "source_scraper": f"PortailEE Scraper",
            "source_scraper_year": item["year"],
            "source_file_url": item["source_file_url"],
            "source_filename": item["source_filename"],
            "source_page_url": item["source_page_url"],
            "project_id": item["project_id"],
        }

        adapter = ItemAdapter(item)
        if adapter.get("departments") and adapter.get("departments_sources"):
            data["departments"] = item["departments"]
            data["departments_sources"] = item["departments_sources"]

        # if item["error"]:
        #   data["_tag"] = "hidden"

        try:
            if not spider.dry_run:
                spider.client.documents.upload(
                    item["local_file_path"],
                    project=spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    publish_at=item["publication_datetime_dcformat"],
                    source="evaluation-environnementale.developpement-durable.gouv.fr",
                    language="fra",
                    access=spider.access_level,
                    data=data,
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)

        else:  # No upload error, add to event_data
            # last_modified = datetime.datetime.strptime(
            #     item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
            # ).isoformat()
            now = datetime.datetime.now().isoformat(timespec="seconds")

            spider.event_data[item["source_file_url"]] = {
                # "last_modified": last_modified,
                "last_seen": now,
                "target_year": item["year"],
                # "run_id": spider.run_id,
            }

            # Save event data after each upload
            if spider.run_id:  # only from the web interface
                spider.store_event_data(spider.event_data)

        return item

    def close_spider(self, spider):
        """Update event data when the spider closes."""

        if not spider.dry_run and spider.run_id:
            spider.store_event_data(spider.event_data)
            spider.logger.info(
                f"Uploaded event data ({len(spider.event_data)} documents)"
            )

            if spider.upload_event_data:
                # Upload the event_data to the DocumentCloud interface
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M")
                filename = f"event_data_PEE_{timestamp}.json"

                with open(filename, "w+") as event_data_file:
                    json.dump(spider.event_data, event_data_file)
                    spider.upload_file(event_data_file)
                spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not spider.run_id:
            with open("event_data.json", "w") as file:
                json.dump(spider.event_data, file)
                spider.logger.info(
                    f"Saved file event_data.json ({len(spider.event_data)} documents)"
                )


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):

        self.items.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            authority: {item["authority"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            if error:
                item_string = item_string + f"\nfull_info: {item['full_info']}"

            return item_string

        subject = f"PortailEE Scraper {str(spider.target_years[0])}-{str(spider.target_years[-1])} (New: {len(self.items)}) [{spider.run_name}]"

        if spider.dry_run:
            subject = "[dry run] " + subject

        content = f"SCRAPED ITEMS ({len(self.items)})\n\n" + "\n\n".join(
            [print_item(item) for item in self.items]
        )

        start_content = f"PortailEE Scraper Addon Run {spider.run_id}"

        content = "\n\n".join([start_content, content])

        if not spider.dry_run:
            spider.send_mail(subject, content)


class DeleteFilesPipeline:

    def process_item(self, item, spider):

        if os.path.isfile(item["local_file_path"]):
            os.remove(item["local_file_path"])

        return item

    def close_spider(self, spider):

        # Delete the downloaded_zips folder
        if os.path.isdir("downloaded_files"):
            shutil.rmtree("downloaded_files")
