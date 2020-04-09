import unittest

from oaipmhserver import domain


class ResumptionTokenInterfaceTest:
    def test_responds_to_encode(self):
        self.assertTrue(hasattr(self.object, "encode"))

    def test_responds_to_decode(self):
        self.assertTrue(hasattr(self.object, "decode"))

    def test_responds_to_next(self):
        self.assertTrue(hasattr(self.object, "next"))


class ResumptionTokenTests(ResumptionTokenInterfaceTest, unittest.TestCase):
    def setUp(self):
        self.object = domain.ResumptionToken()

    def test_token_is_encoded_correctly(self):
        token = domain.ResumptionToken(
            set="",
            from_="1998-01-01",
            until="1998-12-31",
            offset="5dd17ed0d0926d03e0638525",
            count="1000",
            metadataPrefix="oai_dc",
        )
        self.assertEqual(
            token.encode(),
            ",1998-01-01,1998-12-31,5dd17ed0d0926d03e0638525,1000,oai_dc",
        )

    def test_encode_ommit_empty_strings(self):
        token = domain.ResumptionToken(
            set="",
            from_="",
            until="",
            offset="5dd17ed0d0926d03e0638525",
            count="1000",
            metadataPrefix="oai_dc",
        )
        self.assertEqual(token.encode(), ",,,5dd17ed0d0926d03e0638525,1000,oai_dc")

    def test_encode_turns_integer_to_string(self):
        token = domain.ResumptionToken(
            set="",
            from_="1998-01-01",
            until="1998-12-31",
            offset="5dd17ed0d0926d03e0638525",
            count=1000,
            metadataPrefix="oai_dc",
        )
        self.assertEqual(
            token.encode(),
            ",1998-01-01,1998-12-31,5dd17ed0d0926d03e0638525,1000,oai_dc",
        )

    def test_encode_treats_none_as_empty_strings(self):
        token = domain.ResumptionToken(
            set="",
            from_="1998-01-01",
            until="1998-12-31",
            offset="5dd17ed0d0926d03e0638525",
            count=None,
            metadataPrefix="oai_dc",
        )
        self.assertEqual(
            token.encode(), ",1998-01-01,1998-12-31,5dd17ed0d0926d03e0638525,,oai_dc"
        )

    def test_token_is_decoded_correctly(self):
        token = domain.ResumptionToken.decode(
            "foo,1998-01-01,1998-12-31,5dd17ed0d0926d03e0638525,1000,oai_dc"
        )
        self.assertEqual(token.set, "foo")
        self.assertEqual(token.from_, "1998-01-01")
        self.assertEqual(token.until, "1998-12-31")
        self.assertEqual(token.offset, "5dd17ed0d0926d03e0638525")
        self.assertEqual(token.count, "1000")
        self.assertEqual(token.metadataPrefix, "oai_dc")

    def test_decodes_empty_values_to_empty_strings(self):
        token = domain.ResumptionToken.decode(",,,5dd17ed0d0926d03e0638525,1000,oai_dc")
        self.assertEqual(token.set, "")
        self.assertEqual(token.from_, "")
        self.assertEqual(token.until, "")

    def test_next_token(self):
        token = domain.ResumptionToken(
            set="",
            from_="1998-01-01",
            until="1998-12-31",
            offset="5dd17ed0d0926d03e0638524",
            count="2",
            metadataPrefix="oai_dc",
        )
        documents = [
            {"_id": "5dd17ed0d0926d03e0638525"},
            {"_id": "5dd17ed0d0926d03e0638526"},
        ]
        self.assertEqual(
            token.next(documents).encode(),
            ",1998-01-01,1998-12-31,5dd17ed0d0926d03e0638526,2,oai_dc",
        )

    def test_next_token_when_nothing_is_left(self):
        token = domain.ResumptionToken(
            set="",
            from_="1998-01-01",
            until="1998-12-31",
            offset="5dd17ed0d0926d03e0638524",
            count="2",
            metadataPrefix="oai_dc",
        )
        # Observe que a paginação está definida para 2 documentos, mas estamos
        # retornando apenas 1, portanto não haverá uma outra página.
        documents = [{"_id": "5dd17ed0d0926d03e0638525"}]
        self.assertEqual(token.next(documents), None)
