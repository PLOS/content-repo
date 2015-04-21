#!/usr/bin/env python2

"""
"""

__author__ = 'jkrzemien@plos.org'

from abc import ABCMeta, abstractmethod


class AbstractResponse(object):

  __metaclass__ = ABCMeta

  @abstractmethod
  def get_buckets(self):
    pass

  @abstractmethod
  def get_bucketID(self):
    pass

  @abstractmethod
  def get_bucketName(self):
    pass

  @abstractmethod
  def get_bucketTimestamp(self):
    pass

  @abstractmethod
  def get_bucketCreationDate(self):
    pass

  @abstractmethod
  def get_bucketActiveObjects(self):
    pass

  @abstractmethod
  def get_bucketTotalObjects(self):
    pass

  @abstractmethod
  def get_objectKey(self):
    pass

  @abstractmethod
  def get_collections(self):
    pass

  @abstractmethod
  def get_collectionKey(self):
    pass

  @abstractmethod
  def get_collectionVersionNumber(self):
    pass

  @abstractmethod
  def get_collectionUUID(self):
    pass

  @abstractmethod
  def get_collectionStatus(self):
    pass

  @abstractmethod
  def get_repoErrorCode(self):
    pass

  @abstractmethod
  def get_message(self):
    pass

