# General rule
allow(actor, action, resource) if has_permission(actor, action, resource);

# Open bar mode
allow(_actor, _action, _resource) if OpenBarPolarClass.get();

# Admin can do anything
allow(user: UserActor, _action, _resource) if user.is_admin = true;


actor UserActor {}


resource User {
    permissions = ["create", "read", "update", "delete", "set_admin", "set_active"];
    roles = ["self"];

    "read" if "self";
    "update" if "self";
}

has_role(_user: UserActor{id: id}, "self", _user: User{id: id});


resource Campaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["member"];

    "read" if "member";
}

has_role(user: UserActor, "member", campaign: Campaign) if
    ubc in campaign.users_by_campaigns and
    ubc.user = user;


resource UserByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["self"];

    "read" if "self";
}

has_role(user: UserActor, "self", ubc: UserByCampaign) if
    user = ubc.user;


resource TimeseriesByCampaign {
    permissions = ["create", "read", "update", "delete", "read_data", "write_data"];
    relations = {
        campaign: Campaign
    };

    "read" if "member" on "campaign";
    "read_data" if "member" on "campaign";
    "write_data" if "member" on "campaign";
}

has_relation(campaign: Campaign, "campaign", tbc: TimeseriesByCampaign) if
  campaign = tbc.campaign;


resource Timeseries {
    permissions = ["create", "read", "update", "delete"];
    roles = ["reader"];

    "read" if "reader";
}

has_role(user: UserActor, "reader", ts: Timeseries) if
    tbc in ts.timeseries_by_campaigns and
    has_role(user, "member", tbc.campaign);


resource TimeseriesData {
    # Only admin can read/write without specifying a campaign
    permissions = ["read_without_campaign", "write_without_campaign"];
}
