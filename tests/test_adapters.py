import unittest

from oaipmh import adapters


class KernelChangelogStateMachineTests(unittest.TestCase):
    def setUp(self):
        self.state_machine = adapters.KernelChangelogStateMachine()

    def test_initial_state_is_get(self):
        self.assertEqual(self.state_machine.task(), "get")

    def test_get_then_modified_remains_get(self):
        self.state_machine.on_event("modified")
        self.assertEqual(self.state_machine.task(), "get")

    def test_get_then_deleted_turns_delete(self):
        self.state_machine.on_event("deleted")
        self.assertEqual(self.state_machine.task(), "delete")

    def test_deleted_then_deleted_remains_deleted(self):
        self.state_machine.on_event("deleted")
        self.state_machine.on_event("deleted")
        self.assertEqual(self.state_machine.task(), "delete")


class KernelTasksReaderTests(unittest.TestCase):
    def setUp(self):
        self.reader = adapters.KernelTasksReader()

    def test_modified_twice(self):
        changelog = [
            {
                "timestamp": "2018-08-05 23:03:44.971230Z",
                "id": "/documents/0034-8910-rsp-48-2-0347",
            },
            {
                "timestamp": "2018-08-06 08:02:23.743451Z",
                "id": "/documents/0034-8910-rsp-48-2-0347",
            },
        ]
        tasks, _ = self.reader.read(changelog)
        self.assertEqual(
            tasks, [{"id": "/documents/0034-8910-rsp-48-2-0347", "task": "get"}]
        )

    def test_modified_and_deleted(self):
        changelog = [
            {
                "timestamp": "2018-08-05 23:03:44.971230Z",
                "id": "/documents/0034-8910-rsp-48-2-0347",
            },
            {
                "timestamp": "2018-08-06 08:02:23.743451Z",
                "id": "/documents/0034-8910-rsp-48-2-0347",
                "deleted": True,
            },
        ]
        tasks, _ = self.reader.read(changelog)
        self.assertEqual(
            tasks, [{"id": "/documents/0034-8910-rsp-48-2-0347", "task": "delete"}]
        )

    def test_many_documents(self):
        changelog = [
            {
                "timestamp": "2018-08-05 23:03:44.971230Z",
                "id": "/documents/8734-7911-foo-22-1-0013",
            },
            {
                "timestamp": "2018-08-06 08:02:23.743451Z",
                "id": "/documents/0034-8910-rsp-48-2-0347",
                "deleted": True,
            },
        ]
        tasks, _ = self.reader.read(changelog)
        self.assertEqual(
            tasks,
            [
                {"id": "/documents/8734-7911-foo-22-1-0013", "task": "get"},
                {"id": "/documents/0034-8910-rsp-48-2-0347", "task": "delete"},
            ],
        )
