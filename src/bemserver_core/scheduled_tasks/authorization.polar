resource TaskByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", tbc: TaskByCampaign ) if
    has_role(user, "member", tbc.campaign);
