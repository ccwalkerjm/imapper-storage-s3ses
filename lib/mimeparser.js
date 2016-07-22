"use strict";

var addressparser = require("./addressparser");

module.exports = function(rfc822) {
    var parser = new Parser(rfc822),
        response;
    parser.parse();
    parser.finalizeTree();
    response = parser.tree.childNodes[0] || false;
    if (response) {
        response.text = (parser.rawBody || "").replace(/^\r?\n/, "");
    }
    return response;
};

/**
 * Parses a RFC822 message into a structured object (JSON compatible)
 *
 * @constructor
 * @param {String|Buffer} rfc822 Raw body of the message
 */
function Parser(rfc822) {

    // ensure the input is a binary string
    this.rfc822 = (rfc822 || "").toString("binary");

    this._br = "";
    this._pos = 0;

    this.rawBody = "";

    this.tree = {
        childNodes: []
    };
    this._node = this.createNode(this.tree);
}

/**
 * Parses the message, line by line
 */
Parser.prototype.parse = function() {
    var line, prevBr = "";

    // keep parsing until the last linebreak is not a string (no linebreaks anymore)
    while (typeof this._br === "string") {
        line = this.readLine();

        switch (this._node.state) {

            case "header": // process header section
                if (!line) {
                    this.processNodeHeader();
                    this.processContentType();

                    this._node.state = "body";
                } else {
                    this._node.header.push(line);
                }
                break;

            case "body": // process body section
                this.rawBody += prevBr + line;

                if (this._node.parentBoundary && (line === "--" + this._node.parentBoundary || line === "--" + this._node.parentBoundary + "--")) {
                    if (line === "--" + this._node.parentBoundary) {
                        this._node = this.createNode(this._node.parentNode);
                    } else {
                        this._node = this._node.parentNode;
                    }
                } else if (this._node.boundary && line === "--" + this._node.boundary) {
                    this._node = this.createNode(this._node);
                } else {
                    // push the line with previous linebreak value
                    // if the array is joined together to a one string,
                    // then the linebreaks in the string are the "original" ones
                    this._node.body.push((this._node.body.length ? prevBr : "") + line);
                }
                break;

            default: // never should be reached
                throw new Error("Unexpected state");
        }

        // store the linebreak for later usage
        prevBr = this._br;
    }
};

/**
 * Reads a line from the message body
 *
 * @return {String|Boolean} A line from the message
 */
Parser.prototype.readLine = function() {
    var match = this.rfc822.substr(this._pos).match(/(.*?)(\r?\n|$)/);
    if (match) {
        this._br = match[2] || false;
        this._pos += match[0].length;
        return match[1];
    }
    return false;
};

/**
 * Join body arrays into strings. Removes unnecessary fields
 * from the tree (circular references prohibit conversion to JSON)
 */
Parser.prototype.finalizeTree = function() {
    var walker = function(node) {
        if (node.body) {
            node.lineCount = node.body.length;
            node.body = node.body.join("").
                // ensure proper line endings
            replace(/\r?\n/g, "\r\n");
            node.size = node.body.length;
        }
        node.childNodes.forEach(walker);

        // remove unneeded properties
        delete node.parentNode;
        delete node.state;
        if (!node.childNodes.length) {
            delete node.childNodes;
        }
        delete node.parentBoundary;
    };
    walker(this.tree);
};


/**
 * Creates a new node with default values for the parse tree
 */
Parser.prototype.createNode = function(parentNode) {
    var node = {
        state: "header",
        parentNode: parentNode,
        childNodes: [],
        header: [],
        parsedHeader: {},
        body: [],
        multipart: false,
        parentBoundary: parentNode.boundary,
        boundary: false
    };
    parentNode.childNodes.push(node);
    return node;
};

/**
 * Processes header lines. Splits lines to key-value pairs
 * and processes special values
 */
Parser.prototype.processNodeHeader = function() {
    var key, value;

    for (var i = this._node.header.length - 1; i >= 0; i--) {
        if (i && this._node.header[i].match(/^\s/)) {
            this._node.header[i - 1] = this._node.header[i - 1] + "\r\n" + this._node.header[i];
            this._node.header.splice(i, 1);
        } else {
            value = this._node.header[i].split(":");
            key = (value.shift() || "").trim().toLowerCase();
            value = value.join(":").trim();

            if (key in this._node.parsedHeader) {
                if (Array.isArray(this._node.parsedHeader[key])) {
                    this._node.parsedHeader[key].unshift(value);
                } else {
                    this._node.parsedHeader[key] = [value, this._node.parsedHeader[key]];
                }
            } else {
                this._node.parsedHeader[key] = value.replace(/\s*\r?\n\s*/g, " ");
            }
        }
    }

    // always ensure the presence of Content-Type
    if (!this._node.parsedHeader["content-type"]) {
        this._node.parsedHeader["content-type"] = "text/plain";
    }

    // parse additional params for Content-Type and Content-Disposition
    ["content-type", "content-disposition"].forEach((function(key) {
        if (this._node.parsedHeader[key]) {
            this._node.parsedHeader[key] = this.parseValueParams([].concat(this._node.parsedHeader[key] || []).pop());
        }
    }).bind(this));

    // Parse address fields (join several fields with same key)
    ["from", "sender", "reply-to", "to", "cc", "bcc"].forEach((function(key) {
        var addresses = [];
        if (this._node.parsedHeader[key]) {
            [].concat(this._node.parsedHeader[key] || []).forEach(function(value) {
                if (value) {
                    addresses = addresses.concat(addressparser(value) || []);
                }
            });
            this._node.parsedHeader[key] = addresses;
        }
    }).bind(this));
};

/**
 * Splits a value to an object.
 * eg. "text/plain; charset=utf-8" -> {value: "text/plain", params:{charset: "utf-8"}}
 *
 * @param {String} headerValue A string value for a header key
 * @return {Object} Parsed value
 */
Parser.prototype.parseValueParams = function(headerValue) {
    var data = {
            value: "",
            type: "",
            subtype: "",
            params: {}
        },
        match, processEncodedWords = {};

    (headerValue || "").split(";").forEach(function(part, i) {
        var key, value;
        if (!i) {
            data.value = part.trim();
            data.subtype = data.value.split("/");
            data.type = (data.subtype.shift() || "").toLowerCase();
            data.subtype = data.subtype.join("/");
            return;
        }
        value = part.split("=");
        key = (value.shift() || "").trim().toLowerCase();
        value = value.join("=").replace(/^['"\s]*|['"\s]*$/g, "");

        // This regex allows for an optional trailing asterisk, for headers
        // which are encoded with lang/charset info as well as a continuation.
        // See https://tools.ietf.org/html/rfc2231 section 4.1.
        if ((match = key.match(/^([^*]+)\*(\d)?\*?$/))) {
            if (!processEncodedWords[match[1]]) {
                processEncodedWords[match[1]] = [];
            }
            processEncodedWords[match[1]][Number(match[2]) || 0] = value;
        } else {
            data.params[key] = value;
        }
        data.hasParams = true;
    });

    // convert extended mime word into a regular one
    Object.keys(processEncodedWords).forEach(function(key) {
        var charset = "",
            value = "";
        processEncodedWords[key].forEach(function(val) {
            var parts = val.split("'");
            charset = charset || parts.shift();
            value += (parts.pop() || "").replace(/%/g, "=");
        });
        data.params[key] = "=?" + (charset || "ISO-8859-1").toUpperCase() + "?Q?" + value + "?=";
    });

    return data;
};

/**
 * Checks Content-Type value for the current tree node.
 */
Parser.prototype.processContentType = function() {
    if (!this._node.parsedHeader["content-type"]) {
        return;
    }

    if (this._node.parsedHeader["content-type"].type === "multipart" && this._node.parsedHeader["content-type"].params.boundary) {
        this._node.multipart = this._node.parsedHeader["content-type"].subtype;
        this._node.boundary = this._node.parsedHeader["content-type"].params.boundary;
    }
};