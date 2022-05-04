"""BEMServer core base processor class"""
import logging

from bemserver_core.model import Processor, ProcessorByCampaign
from bemserver_core.database import db


class BEMServerCoreProcessor:
    """Base class for BEMServer core processors"""

    #: Unique string ID identifying the processor (max. 40 chars)
    STR_ID = ""
    #: Name of the processor (max. 80 chars)
    NAME = ""
    #: Description of the processor (max. 50 chars)
    DESCRIPTION = ""
    #: Schedule parameters
    SCHEDULE_TYPE = "interval"
    SCHEDULE_KWARGS = {"seconds": 10}

    def __init__(self):
        self.logger = logging.getLogger(f"bemserver-processor-{self.STR_ID}")

    @property
    def db_processor(self):
        return db.session.query(Processor).get(self.STR_ID)

    def run(self):
        """Run processor

        By default, this calls run_for_campaign for every campaign on which
        the processor is configured to run.
        """
        self.logger.info("Run")
        for pbc in ProcessorByCampaign.get():
            self.run_for_campaign(pbc.campaign)

    def run_for_campaign(self, campaign):
        """Run processor for a given campaign

        :param Campaign campaign: Campaign to run processor on
        """
