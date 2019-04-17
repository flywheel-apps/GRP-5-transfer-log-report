import flywheel
import pytest
import mock
import utils


class MockContainer(object):
    def __init__(self, container_type):
        self.parents = {'group': 'group_id'}
        if container_type in ['subject', 'session', 'acquisition']:
            self.parents['project'] = 'project_id'
        if container_type in ['session', 'acquisition']:
            self.parents['subject'] = 'subject_id'
        if container_type == 'acquisition':
            self.parents['session'] = 'session_id'
        self.label = '{}_label'.format(container_type)
        self.id = '{}_id'.format(container_type)


class MockParent(object):
    count = 0
    containers = ['group', 'project', 'subject', 'session']
    def __init__(self):
        self.id = '{}_id'.format(MockParent.containers[MockParent.count])
        self.label = '{}_label'.format(MockParent.containers[MockParent.count])
        MockParent.count = (MockParent.count + 1) % len(MockParent.containers)


class MockClient(object):
    def get(self, _id):
        return MockParent()


def test_set_path_for_project():
    client = MockClient()
    project = MockContainer('project')
    MockParent.count = 0

    resolve_path = utils.get_resolver_path(client, project)
    assert resolve_path == 'group_id/project_label'


def test_set_path_for_subject():
    client = MockClient()
    subject = MockContainer('subject')
    MockParent.count = 0

    resolve_path = utils.get_resolver_path(client, subject)
    assert resolve_path == 'group_id/project_label/subject_label'


def test_set_path_for_session():
    client = MockClient()
    session = MockContainer('session')
    MockParent.count = 0

    resolve_path = utils.get_resolver_path(client, session)
    assert resolve_path == 'group_id/project_label/subject_label/session_label'


def test_set_path_for_acquisition():
    client = MockClient()
    acquisition = MockContainer('acquisition')
    MockParent.count = 0

    resolve_path = utils.get_resolver_path(client, acquisition)
    assert resolve_path == 'group_id/project_label/subject_label/session_label/acquisition_label'
