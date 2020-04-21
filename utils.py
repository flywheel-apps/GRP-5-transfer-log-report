def get_resolver_path(client, container):
    """Generates the resolver path for a container

    Args:
        client (Client): Flywheel Api client
        container (Container): A flywheel container

    Returns:
        str: A human-readable resolver path that can be used to find the
            container
    """
    resolver_path = []
    for parent_type in ['group', 'project', 'subject', 'session']:
        parent_id = container.parents.get(parent_type)
        if parent_id:
            if parent_type == 'group':
                path_part = client.get(parent_id).id
            else:
                path_part = client.get(parent_id).label
            resolver_path.append(path_part)
        else:
            break
    resolver_path.append(container.label)
    return '/'.join(resolver_path)


def set_resolver_paths(error_containers, client):
    """Sets the resolver path for the list of error containers

    Args:
        error_containers (list): list of container dictionaries
        client (Client): Flywheel Api client
    """
    for error_container in error_containers:
        container = client.get(error_container['_id'])
        error_container['path'] = get_resolver_path(client, container)
