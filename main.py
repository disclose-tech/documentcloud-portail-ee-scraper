"""
Disclose's custom scraper add-on for DocumentCloud.
"""

import datetime
import os
import sys
from urllib.parse import urlparse
import logging

from documentcloud import DocumentCloud
from documentcloud.addon import AddOn

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scraper import settings as scraper_settings
from scraper.spiders.pee import PEESpider


class DisclosePEEScraper(AddOn):
    """Scraper for Portail EE documents (https://evaluation-environnementale.developpement-durable.gouv.fr)."""

    def check_permissions(self):
        """Check if the user is a verified journalist & can upload a document."""

        self.set_message("Checking permissions...")

        try:
            user = self.client.users.get("me")

            if not user.verified_journalist:
                self.set_message(
                    "You need to be verified to use this add-on. Please verify your "
                    "account here: https://airtable.com/shrZrgdmuOwW0ZLPM"
                )
                self.send_mail(
                    "You must verify your account to use Disclose's Scraper Add-On",
                    "You need to be verified to use the scraper add-on. Please verify your "
                    "account here: https://airtable.com/shrZrgdmuOwW0ZLPM",
                )

                sys.exit(1)
        except Exception as e:
            subject = f"PEE Scraper - Error connecting to DocumentCloud"
            content = f"Error:\n {e.__traceback__}"
            self.send_mail(subject, content)
            sys.exit(1)

    def check_access_level(self):
        """Check that the access level is valid."""

        access_level = self.access_level

        if access_level not in ["public", "organization", "private"]:
            self.set_message(
                "Incorrect Access level.",
                "Must be 'public', 'organization' or 'private'.",
            )
            sys.exit(1)

    def get_project_id(self):
        """Returns the id of the target project."""

        project = self.data["project"]

        try:
            # if project is an integer, use it as a project ID
            project = int(project)
            return project
        except ValueError:
            # otherwise, get the project id from its title
            # or create it if it does not exist
            project, created = self.client.projects.get_or_create_by_title(project)
            return project.id

    def main(self):
        """Add-on main functionality."""

        # Add-on inputs

        self.run_name = self.data.get("run_name", "no name")

        self.access_level = self.data["access_level"]

        self.from_year = self.data.get("from_year", 2024)  # 2024 = beginning of data

        self.upload_limit = self.data.get("upload_limit", 0)
        self.time_limit = self.data.get(
            "time_limit", 345
        )  # Default to 5h45 as Github actions have a 6 hour limit

        self.upload_event_data = self.data.get("upload_event_data")

        self.dry_run = self.data.get("dry_run")

        if not self.dry_run:
            try:
                self.project = self.get_project_id()
            except Exception as e:
                raise Exception("Project error").with_traceback(e.__traceback__)
                sys.exit(1)
                # TODO : check user has access to the project?

            # Check if the user has upload permissions (verified account)
            self.check_permissions()
        else:
            self.project = ""

        # Event data

        # Load scraper settings and create process

        os.environ.setdefault("SCRAPY_SETTINGS_MODULE", scraper_settings.__name__)
        process = CrawlerProcess(get_project_settings())

        # Launch scraper

        process.crawl(
            PEESpider,
            from_year=self.from_year,
            upload_limit=self.upload_limit,
            time_limit=self.time_limit,
            client=self.client,
            target_project=self.project,
            access_level=self.access_level,
            dry_run=self.dry_run,
            run_id=self.id,
            run_name=self.run_name,
            send_mail=self.send_mail,
            load_event_data=self.load_event_data,
            store_event_data=self.store_event_data,
            upload_file=self.upload_file,
            upload_event_data=self.upload_event_data,
        )

        # Run

        self.set_message(
            f"Scraping Portail EE documents {str(self.from_year)}+ [{self.run_name}]"
        )
        process.start()
        self.set_message("Scraping complete!")


if __name__ == "__main__":
    DisclosePEEScraper().main()
