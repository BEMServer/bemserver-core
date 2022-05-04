from .base import BEMServerCoreProcessor


class CleanupProcessor(BEMServerCoreProcessor):

    STR_ID = "cleanup"
    NAME = "Cleanup Processor"
    DESCRIPTION = "Processor generating clean data from raw data by removing outliers"

    def run_for_campaign(self, campaign):
        self.logger.info(f"Run for campaign {campaign.name}")
        for cs in campaign.campaign_scopes:
            for timeseries in cs.timeseries:
                self.logger.info(f"Timeseries {timeseries.name}")
