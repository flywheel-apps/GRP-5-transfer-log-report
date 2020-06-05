import backoff
import flywheel


def get_resolver_path(client, container):
    """Generates the resolveer path for a container

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


def false_if_status_gte_500(exception):
    """
    A giveup function to be passed as giveup parameer to  backoff.on_exception
        Give up for status codes below 500, backoff for >= 500

    Args:
        exception (flywheel.rest.ApiException): a flywheel API exception

    Returns:
        bool: whether to raise rather than backing off
    """
    if exception.status >= 500:
        return False
    return True


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_status_gte_500)
def get_resolver_path_for_id(container_id, fw_client):
    if container_id:
        try:
            container = fw_client.get(container_id)
            resolver_path = get_resolver_path(fw_client, container)
            return resolver_path
        except flywheel.ApiException as exc:
            if exc.status == 404:
                return None
            else:
                raise exc
    else:
        return None


def get_project_resolver_path_dict(fw_client, project_id):
    """
    Prepares a dictionary with container id: resolver path key:value pairs for
        all containers within the project with project_id

    Args:
        fw_client (flywheel.Client): an instance of the Flywheel client
        project_id: an id belonging to a Flywheel project

    Returns:
        dict: a dictionary with container id: resolver path key:value pairs
    """
    path_dict = dict()
    project = fw_client.get_project(project_id)
    group_id = project.group
    path_dict[group_id] = group_id
    project_path = '/'.join([group_id, project.label])
    path_dict[project_id] = project_path
    for subject in project.subjects.iter():
        subject_path = '/'.join([project_path, subject.label])
        path_dict[subject.id] = subject_path
        for session in subject.sessions.iter():
            session_path = '/'.join([subject_path, session.label])
            path_dict[session.id] = session_path
            for acquisition in session.acquisitions.iter():
                acquisition_path = '/'.join([session_path, acquisition.label])
                path_dict[acquisition.id] = acquisition_path
    return path_dict


def get_label_view(fw_client, project_id):
    columns = ['acquisition.label', 'session']