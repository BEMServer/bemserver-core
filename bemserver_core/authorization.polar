# General rule
allow(actor, action, resource) if has_permission(actor, action, resource);

# Open bar mode
allow(_actor, _action, _resource) if OpenBarPolarClass.get();

# Admin can do anything
allow(user: User, _action, _resource) if user.is_admin = true;

# User has role "user" on anything
resource Base {
    roles = ["user"];
}

has_role(_: User, "user", _: Base);


actor User {}
