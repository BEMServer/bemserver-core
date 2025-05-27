"""Test task by campaign association"""

import datetime as dt
from unittest import mock
from zoneinfo import ZoneInfo

import pytest

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.model import TaskByCampaign
from bemserver_core.time_utils import PeriodEnum


class TestTaskByCampaignModel:
    @pytest.mark.usefixtures("tasks_by_campaigns")
    def test_task_by_campaign_delete_cascade(self, users, campaigns):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            assert len(list(TaskByCampaign.get())) == 2
            campaign_1.delete()
            db.session.commit()
            assert len(list(TaskByCampaign.get())) == 1

    def test_task_by_campaign_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            st_cbc_1 = TaskByCampaign.new(
                task_name="TASK",
                campaign_id=campaign_1.id,
                offset_unit=PeriodEnum.day,
            )
            db.session.commit()
            st_cbc = TaskByCampaign.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(TaskByCampaign.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(campaign_id=campaign_2.id)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_task_by_campaign_authorizations_as_user(
        self, users, campaigns, tasks_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        st_cbc_1 = tasks_by_campaigns[0]
        st_cbc_2 = tasks_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TaskByCampaign.new(
                    task_name="TASK",
                    campaign_id=campaign_2.id,
                    offset_unit=PeriodEnum.day,
                )
            with pytest.raises(BEMServerAuthorizationError):
                TaskByCampaign.get_by_id(st_cbc_1.id)
            TaskByCampaign.get_by_id(st_cbc_2.id)
            stcs = list(TaskByCampaign.get())
            assert stcs == [st_cbc_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.delete()

    def test_task_by_campaign_make_interval(self, campaigns):
        campaign_1 = campaigns[0]
        with OpenBar():
            tbc_1 = TaskByCampaign.new(
                task_name="TASK",
                campaign=campaigns[0],
                offset_unit=PeriodEnum.day,
                start_offset=-1,
                end_offset=2,
            )
        db.session.flush()

        with mock.patch("datetime.datetime", wraps=dt.datetime) as dt_patch:
            dt_patch.now.return_value = dt.datetime(
                2020, 1, 2, tzinfo=ZoneInfo(campaign_1.timezone)
            )
            start_dt, end_dt = tbc_1.make_interval()

        assert start_dt == dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        assert end_dt == dt.datetime(2020, 1, 4, tzinfo=dt.timezone.utc)
