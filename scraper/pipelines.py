# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json
import hashlib
import shutil
import sys

from itemadapter import ItemAdapter

from scrapy.exceptions import DropItem

from documentcloud.constants import SUPPORTED_EXTENSIONS

from .log import SilentDropItem
from .departments import department_from_authority, departments_from_project_name
from .event_data import load_archive, normalize_event_data


class SpiderPipeline:
    """Base class for pipelines that need access to the spider instance.

    Provides from_crawler() to store spider as self.spider.
    Inherit from this class instead of defining from_crawler() in each pipeline.
    """

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.spider = crawler.spider
        return pipeline


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item):
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

    def process_item(self, item):

        filename, file_extension = os.path.splitext(item["source_filename"])
        file_extension = file_extension.lower()

        if file_extension not in SUPPORTED_EXTENSIONS:
            # Drop the item
            raise SilentDropItem("Unsupported filetype")
        else:
            return item


class BeautifyPipeline:
    def process_item(self, item):
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


class CategoryPipeline(SpiderPipeline):
    """Attributes the final category of the document."""

    def process_item(self, item):

        spider = self.spider

        if "cas par cas" in item["category_local"].lower():
            item["category"] = "Cas par cas"

        elif item["category_local"].startswith("Demande d'avis"):
            item["category"] = "Avis"

        elif item["category_local"] in ["Avis", "Avis Projet"]:
            item["category"] = "Avis"

        else:
            subject = "Maintenance needed on portail-ee-scraper"
            content = f"A new category has appeared: {item['category_local']}. Modify the code to import the documents in the correct category."
            spider.send_mail(subject, content)
            raise DropItem("Unknown category_local")

        return item


class UploadLimitPipeline(SpiderPipeline):
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self):
        self.number_of_docs = 0

    def process_item(self, item):
        spider = self.spider
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class TagDepartmentsPipeline:

    def process_item(self, item):

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

    def process_item(self, item):

        project_name = item["project"]
        source_page_url = item["source_page_url"]
        string_to_hash = source_page_url + " " + project_name

        hash_object = hashlib.sha256(string_to_hash.encode())
        hex_dig = hash_object.hexdigest()

        item["project_id"] = hex_dig

        return item


class UploadPipeline(SpiderPipeline):
    """Upload document to DocumentCloud & store event data."""

    # Number of uploads between two saves of the event data on DocumentCloud
    EVENT_DATA_SAVE_INTERVAL = 25

    def open_spider(self):
        spider = self.spider
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)
        squarelet_logger = logging.getLogger("squarelet")
        squarelet_logger.setLevel(logging.WARNING)

        # True when spider.event_data differs from what is stored on DocumentCloud
        self.has_unsaved_changes = False
        self.unsaved_uploads = 0

        # Archived event data (loaded from disk, never stored on DocumentCloud)
        spider.archived_event_data = load_archive()
        spider.logger.info(
            f"Loaded archived event data ({len(spider.archived_event_data)} documents)"
        )

        if not spider.dry_run:
            try:
                spider.logger.info("Loading event data from DocumentCloud...")
                event_data = spider.load_event_data()
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
                    event_data = json.load(file)
            except:
                event_data = {}

        if event_data:
            spider.logger.info(f"Loaded event data ({len(event_data)} documents)")
        else:
            spider.logger.info("No event data was loaded.")
            event_data = {}

        # Convert legacy keys (full download URLs) to file IDs
        spider.event_data = normalize_event_data(event_data)
        if spider.event_data.keys() != event_data.keys():
            spider.logger.info("Converted event data keys to file IDs.")
            self.has_unsaved_changes = True

        # Remove the entries that have been archived
        archived = spider.event_data.keys() & spider.archived_event_data.keys()
        if archived:
            for file_id in archived:
                del spider.event_data[file_id]
            spider.logger.info(
                f"Removed {len(archived)} archived documents from event data "
                f"({len(spider.event_data)} remaining)"
            )
            self.has_unsaved_changes = True

    def store_event_data(self):
        """Stores the event data on DocumentCloud (only from the web interface)."""

        spider = self.spider

        if spider.dry_run or not spider.run_id:
            return

        try:
            spider.store_event_data(spider.event_data)
        except Exception as e:
            # Kept in memory: stored with the next batch or when the spider closes
            spider.logger.warning(f"Error storing event data: {e!r}")
        else:
            self.has_unsaved_changes = False
            self.unsaved_uploads = 0
            spider.logger.info(
                f"Stored event data ({len(spider.event_data)} documents, "
                f"{len(json.dumps(spider.event_data)) / 1000:.1f} KB)"
            )

    def process_item(self, item):

        spider = self.spider

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

            spider.event_data[str(item["file_id"])] = {
                # "last_modified": last_modified,
                "last_seen": now,
                "target_year": item["year"],
                # "run_id": spider.run_id,
            }
            self.has_unsaved_changes = True
            self.unsaved_uploads += 1

            # Save event data by batches of uploads
            if self.unsaved_uploads >= self.EVENT_DATA_SAVE_INTERVAL:
                self.store_event_data()

        return item

    def close_spider(self):
        """Update event data when the spider closes."""

        spider = self.spider

        if not spider.dry_run and spider.run_id:
            if self.has_unsaved_changes:
                self.store_event_data()
            else:
                spider.logger.info("No changes to event data, not storing it.")

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


class MailPipeline(SpiderPipeline):
    """Send scraping run report."""

    def open_spider(self):
        self.items = []

    def process_item(self, item):

        self.items.append(item)

        return item

    def close_spider(self):

        spider = self.spider

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

        if len(self.spider.target_years) == 1:
            year_range_str = str(self.spider.target_years[0])
        else:
            year_range_str = f"{str(self.spider.target_years[0])}-{str(self.spider.target_years[-1])}"

        subject = f"PortailEE Scraper {year_range_str} (New: {len(self.items)}) [{spider.run_name}]"

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

    def process_item(self, item):

        if os.path.isfile(item["local_file_path"]):
            os.remove(item["local_file_path"])

        return item

    def close_spider(self):

        # Delete the downloaded_zips folder
        if os.path.isdir("downloaded_files"):
            shutil.rmtree("downloaded_files")
