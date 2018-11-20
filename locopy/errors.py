# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2018 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class LocopyError(Exception):
    """
    Baseclass for all Locopy errors.
    """


class CompressionError(LocopyError):
    """
    Raised when there is an error compressing a file.
    """


class LocopySplitError(LocopyError):
    """
    Raised when there is an error splitting a file.
    """


class DBError(Exception):
    """
    Base class for all Database errors.
    """


class CredentialsError(DBError):
    """
    Raised when the users credentials are not provided.
    """


class S3Error(Exception):
    """
    Base class for all S3 errors.
    """


class S3CredentialsError(S3Error):
    """
    Raised when there is an error with AWS credentials.
    """


class S3InitializationError(S3Error):
    """
    Raised when there is an error initializing S3 client.
    """


class S3UploadError(S3Error):
    """
    Raised when there is an upload error to S3.
    """


class S3DownloadError(S3Error):
    """
    Raised when there is an download error to S3.
    """


class S3DeletionError(S3Error):
    """
    Raised when there is an deletion error on S3.
    """
