import unittest

import catalog_recovery


class RecoveryHelpersTest(unittest.TestCase):
    def test_uuid7_has_expected_version_and_variant(self):
        value = catalog_recovery.uuid.UUID(catalog_recovery.uuid7())
        self.assertEqual(value.version, 7)
        self.assertEqual(value.variant, catalog_recovery.uuid.RFC_4122)

    def test_idempotency_advertisement_locations(self):
        self.assertTrue(catalog_recovery.advertised_idempotency({"idempotency-key-lifetime": "PT1M"}))
        self.assertTrue(catalog_recovery.advertised_idempotency({"defaults": {"idempotency-key-lifetime": "PT1M"}}))
        self.assertTrue(catalog_recovery.advertised_idempotency({"overrides": {"idempotency-key-lifetime": "PT1M"}}))
        self.assertFalse(catalog_recovery.advertised_idempotency({"defaults": {}}))

    def test_create_and_commit_are_standard_iceberg_shapes(self):
        created = catalog_recovery.create_body("events", "s3://warehouse/events")
        self.assertEqual(created["schema"]["fields"][0]["id"], 1)
        metadata = {"table-uuid": "00000000-0000-0000-0000-000000000001", "current-schema-id": 0}
        commit = catalog_recovery.commit_body(metadata, "phase3", "accepted")
        self.assertEqual(commit["requirements"][0]["type"], "assert-table-uuid")
        self.assertEqual(commit["updates"][0]["action"], "set-properties")

    def test_hashes_never_return_raw_values(self):
        value = "private-value"
        digest = catalog_recovery.sha256(value)
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")
        self.assertNotIn(value, digest)


if __name__ == "__main__":
    unittest.main()
