# imapper-storage-s3ses
This module is an S3 storage plugin for [imapper](https://www.npmjs.com/package/imapper). It uses S3 buckets to store the incoming messages and the mailbox metadata. It uses the codebase from [imapper-storage-memory](https://www.npmjs.com/package/imapper-storage-memory) and adds an S3 interface for message data access.

This module is **not** meant to be a complete S3 storage solution. It's meant to be used along with [imapper-auth-s3](https://www.npmjs.com/package/imapper-auth-s3) and [SES S3 Lambda](https://github.com/seelang2/ses-s3lambda) for use with [Amazon Simple Email Service (SES)](https://aws.amazon.com/ses/). SES provides outbound email SMTP server, but lacks an inbound IMAP server. Incoming messages are stored in S3 buckets.

## Usage
Install with `npm install`:

````bash
npm install imapper-storage-s3ses --save
````

Import the auth module and configure the S3 options: 
```javascript
var storageModule = require("imapper-storage-s3ses");
storageModule.setS3Options('S3BucketName', 'YourS3Bucket');
```
Then add the auth module to the imapper configuration options:
```javascript
var options = {
  storage: storageModule,
    ...
};
var server = imapper(options);
server.listen(143);
```
See the [imapper documentation](https://www.npmjs.com/package/imapper) for more details.

## Configuration Options
There are four configuration parameters that can be set (or alternatively, hardcoded to change the defaults) using `storageModule.setS3Options()`:

* `S3BucketName` : The S3 bucket where the mailbox metadata is stored. No default value.
* `S3MboxKeySuffix` : The account metadata file suffix. Default is '.mbox.json'.
* `S3MessageListKeySuffix` : The account message index file suffix. Default is  '.messagelist.json'.
* `S3MboxBucketSuffix` : The suffix used for the account S3 bucket containing the messages. Default is '.ses.inbound'.

To change the default values, change the properties of the `S3Handler.options` object in `/lib/storage.js`.

## Debug
imapper-storage-s3ses has a verbose mode where debugging information is sent to the console. This can be activated or deactivated by passing `true` or `false` to `storageModule.setConsoleMessages()`.

## Reference
See the documentation for [imapper-storage-memory](https://www.npmjs.com/package/imapper-storage-memory) for additional information about the base module code.

## License
**MIT**
