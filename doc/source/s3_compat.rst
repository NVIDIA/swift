S3/Swift REST API Comparison Matrix
===================================

General compatibility statement
-------------------------------

S3 is a product from Amazon, and as such, it includes "features" that
are  outside the scope of Swift itself. For example, Swift doesn't
have anything to do with billing, whereas S3 buckets can be tied to
Amazon's billing system. Similarly, log delivery is a service outside
of Swift. It's entirely possible for a Swift deployment to provide that
functionality, but it is not part of Swift itself. Likewise, a Swift
deployment can provide similar geographic availability as S3, but this
is tied to the deployer's willingness to build the infrastructure and
support systems to do so.

Amazon S3 operations
---------------------

+------------------------------------------------+------------------+--------------+
| S3 REST API method                             | Category         | Swift S3 API |
+================================================+==================+==============+
| `GET Object`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `HEAD Object`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object Copy`_                             | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Object checksums`_                            | Core-API         | Partial      |
+------------------------------------------------+------------------+--------------+
| `DELETE Object`_                               | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Initiate Multipart Upload`_                   | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Upload Part`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Upload Part Copy`_                            | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Complete Multipart Upload`_                   | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Abort Multipart Upload`_                      | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `List Parts`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Object ACL`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object ACL`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Bucket`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Bucket List Objects`_                     | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `HEAD Bucket`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `DELETE Bucket`_                               | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `List Multipart Uploads`_                      | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Bucket acl`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Bucket acl`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Versioning`_                                  | Versioning       | Yes          |
+------------------------------------------------+------------------+--------------+
| `Bucket notification`_                         | Notifications    | No           |
+------------------------------------------------+------------------+--------------+
| Bucket Lifecycle [1]_ [2]_ [3]_ [4]_ [5]_ [6]_ | Bucket Lifecycle | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket policy`_                               | Advanced ACLs    | No           |
+------------------------------------------------+------------------+--------------+
| Public website [7]_ [8]_ [9]_ [10]_            | Public Website   | No           |
+------------------------------------------------+------------------+--------------+
| Billing [11]_ [12]_                            | Billing          | No           |
+------------------------------------------------+------------------+--------------+
| `GET Bucket location`_                         | Advanced Feature | Yes          |
+------------------------------------------------+------------------+--------------+
| `Delete Multiple Objects`_                     | Advanced Feature | Yes          |
+------------------------------------------------+------------------+--------------+
| `Object tagging`_                              | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `GET Object torrent`_                          | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket inventory`_                            | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `GET Bucket service`_                          | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket accelerate`_                           | CDN Integration  | No           |
+------------------------------------------------+------------------+--------------+

Object checksum support
-----------------------

A subset of the AWS checksum feature is implemented, including:

* Validation of an ``x-amz-checksum-*`` value supplied with `PUT Object`_,
  `Upload Part`_, and `Delete Multiple Objects`_ requests using CRC32, CRC32C,
  CRC64NVME, SHA1 or SHA256 algorithms. This covers both a checksum header on a
  fixed-length body and a checksum trailer on a SigV4 ``aws-chunked`` body.

* Optional persistence of a validated checksum for a `PUT Object`_,
  `Upload Part`_, or `Complete Multipart Upload`_ request, and settings for an
  `Initiate Multipart Upload`_ request.
  Set ``allow_store_checksums = true`` in ``[filter:s3api]`` to enable it.
  The default is ``false``. When enabled, the checksum header is returned
  with the `PUT Object`_ response along with
  ``x-amz-checksum-type: FULL_OBJECT``. S3API registers the checksum value as
  :ref:`sensitive sysmeta <sensitive_sysmeta>` even when persistence is off.
  Before you enable it, upgrade all applicable proxy and internal applications
  that read, write, or copy these objects. Keep S3API registration and COPY
  cleanup in each applicable process. Drain old workers, then enable the
  option and reload the applications. Drain checksum-aware multipart uploads
  before you disable the option or roll back the checksum code. Disabling the
  option stops new checksum writes. Existing checksum reads, decryption,
  stored MPU validation, and COPY protection continue.
  An upload part stores and returns its checksum only when the MPU has
  stored checksum settings and the option is enabled. Parts of legacy MPUs
  still validate a supplied checksum, but do not store or return it.
  Uploads without stored checksum settings remain legacy uploads. Their
  completion can include checksum fields with the option on or off. These
  fields do not enable checksum comparison or storage. Request-format checks
  still apply. Ordinary ETag-only completion remains supported.

* Selection of a checksum algorithm and type for `Initiate Multipart Upload`_.
  ``COMPOSITE`` supports CRC32, CRC32C, SHA1, and SHA256. ``FULL_OBJECT``
  supports CRC32, CRC32C, and CRC64NVME.
  ``COMPOSITE`` uploads require the selected checksum on each part.
  ``FULL_OBJECT`` uploads can omit the part checksum; Swift calculates it.

* Validation of an ``x-amz-checksum-*`` value supplied with a
  `Complete Multipart Upload`_ request.
  If a part has no stored checksum, completion keeps the compatibility
  behavior and writes no final checksum. The response has no checksum fields.

* Return of a checksum header with a `GET Object`_ or `HEAD Object`_ response
  when the client sends ``x-amz-checksum-mode: ENABLED`` and the checksum has
  previously been persisted. Any other value for the ``x-amz-checksum-mode``
  header is rejected with ``InvalidRequest``. Note that a checksum header will
  not be returned for objects stored prior to checksum persistence support.

The following checksum features, and any not mentioned above, are not yet
supported:

* Re-calculation of checksum values during `PUT Object Copy`_ and
  `Upload Part Copy`_ requests.

* Return of checksum metadata for `List Parts`_ or individual parts in
  `GET Object`_ and `HEAD Object`_ responses.

* Inclusion of the per-object checksum algorithm and type in bucket listing
  responses.

----

.. _GET Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGET.html
.. _HEAD Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectHEAD.html
.. _PUT Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUT.html
.. _PUT Object Copy: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectCOPY.html
.. _DELETE Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectDELETE.html
.. _Initiate Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadInitiate.html
.. _Upload Part: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadUploadPart.html
.. _Upload Part Copy: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
.. _Complete Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadComplete.html
.. _Abort Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadAbort.html
.. _List Parts: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadListParts.html
.. _GET Object ACL: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGETacl.html
.. _PUT Object ACL: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUTacl.html
.. _Delete Multiple Objects: http://docs.amazonwebservices.com/AmazonS3/latest/API/multiobjectdeleteapi.html
.. _GET Object torrent: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGETtorrent.html
.. _Object tagging: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html

.. _PUT Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUT.html
.. _GET Bucket List Objects: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
.. _HEAD Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketHEAD.html
.. _DELETE Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketDELETE.html
.. _List Multipart Uploads: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadListMPUpload.html
.. _GET Bucket acl: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETacl.html
.. _PUT Bucket acl: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUTacl.html
.. _Bucket notification: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETnotification.html
.. _Bucket policy: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETpolicy.html
.. _GET Bucket location: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlocation.html
.. _Bucket accelerate: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETaccelerate.html
.. _Bucket inventory: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETInventoryConfig.html
.. _GET Bucket service: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html

.. Versioning
.. _Versioning: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETversioningStatus.html

.. Object checksums
.. _Object checksums: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html


.. Lifecycle
.. [1] `POST restore <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOSTrestore.html>`_
.. [2] `Bucket lifecycle <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlifecycle.html>`_
.. [3] `Bucket logging <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlogging.html>`_
.. [4] `Bucket analytics <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETAnalyticsConfig.html>`_
.. [5] `Bucket metrics <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETMetricConfiguration.html>`_
.. [6] `Bucket replication <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETreplication.html>`_


.. Public website
.. [7] `OPTIONS object <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTOPTIONSobject.html>`_
.. [8] `Object POST from HTML form <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPOST.html>`_
.. [9] `Bucket public website <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETwebsite.html>`_
.. [10] `Bucket CORS <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETcors.html>`_


.. Billing
.. [11] `Request payment <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTrequestPaymentPUT.html>`_
.. [12] `Bucket tagging <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETtagging.html>`_
