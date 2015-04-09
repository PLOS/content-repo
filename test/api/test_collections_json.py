#!/usr/bin/env python2

__author__ = 'gabriela.filomeno'

'''
Test cases for Content Repo Collections requests.
'''
from ..api.RequestObject.collections_json import CollectionsJson


class GetCollections(CollectionsJson):

    def test_collections(self):
        """
        Post Collections API call
        """
        self.post_collection()
        self.verify_post_collections()
        """
        Get Collections API call
        """
        self.get_collections()
        self.verify_list_collections()

if __name__ == '__main__':
    CollectionsJson._run_tests_randomly()