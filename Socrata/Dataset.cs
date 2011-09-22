/*

Copyright (c) 2010 Socrata.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;
using System.IO;

namespace Socrata {
    /// <summary>
    /// A Socrata dataset and associated rows/columns/metadata
    /// </summary>
    public class Dataset : ApiBase {
        private string _uid;
        private static readonly Regex   UID_PATTERN          = new Regex("[a-z0-9]{4}-[a-z0-9]{4}");
        private static readonly int     DEFAULT_COLUMN_WIDTH = 100;
        private static readonly string  DEFAULT_COLUMN_TYPE  = "text";


        /// <summary>
        /// Creates a new, blank dataset.
        /// </summary>
        /// <param name="name">The name of the dataset (must be unique)</param>
        /// <param name="description">On optional description</param>
        /// <returns></returns>
        public bool create(String name, String description) {
            // The data we will send via a POST request
            JObject data = new JObject();
            data.Add("name", name);
            if (description != null) {
                data.Add("description", description);
            }

            JsonPayload response = PostRequest("/views.json", data.ToString(Formatting.None, null));
            if (response == null) {
                _log.Error("Received null response trying to create dataset.");
                return false;
            }
            // Read the UID off the response here...
            if (responseIsClean(response)) {
                if (response.JsonObject != null) {

                    string uid = (string) response.JsonObject["id"];
                    if (isValidId(uid)) {
                        this._uid = uid;
                        _log.Info("Successfully created dataset (" + uid + ")");
                        return true;
                    }
                    else {
                        _log.Error("Received invalid UID in response for dataset creation: '" + uid + "'");
                        return false;
                    }
                }
                else {
                    _log.Error("Error creating dataset: null JSON response, no error message.");
                    return false;
                }
            }
            else {
                return false;
            }
        }

        /// <summary>
        /// Creates a new, blank dataset with only a name.
        /// </summary>
        /// <param name="name">The title of the dataset</param>
        /// <returns>Success or failure</returns>
        public bool create(String name) {
            return create(name, "");
        }

        /// <summary>
        /// Creates a new dataset by importing a file
        /// </summary>
        /// <param name="filename">The location of the file on disk</param>
        /// <returns>Success or failure</returns>
        public bool importFile(String filename) {
            JObject response = multipartUpload("/imports", filename);
            if (response == null) {
                _log.Error("Received a null response after file import.");
                return false;
            }
            else {
                string id = (string)response["id"];
                if (isValidId(id)) {
                    _uid = id;
                    _log.Info("Successfully imported file '" + (string)response["name"] +
                        "' (" + _uid + ")");
                    return true;
                }
                else {
                    _log.Error("Received invalid UID in response to file upload: '" + id + "'");
                    return false;
                }
            }
        }

        /// <summary>
        /// Replaces the data in the existing attached dataset with the contents of the file
        /// </summary>
        /// <param name="filename">The location of the file to overwrite the dataset with</param>
        /// <returns>Whether it worked</returns>
        public bool refresh(String filename)
        {
            return this.refresh(filename, false);
        }

        /// <summary>
        /// Replaces the data in the existing attached dataset with the contents of the file
        /// </summary>
        /// <param name="filename">The location of the file to overwrite the dataset with</param>
        /// <param name="skipHeaders">Whether to skip the first row of your import file</param>
        /// <returns>Whether it worked</returns>
        public bool refresh(String filename, bool skipHeaders) {
            var endpointName = new StringBuilder("replace");
            if (skipHeaders)
                endpointName.Append("?skip_headers=true");
            return multipartAppendOrRefresh(filename, endpointName.ToString());
        }

        /// <summary>
        /// Append the data in the given file to an existing dataset
        /// </summary>
        /// <param name="filename">The location of the file on disk</param>
        /// <returns>Whether it worked</returns>
        public bool append(String filename) {
            return this.append(filename, false);
        }

        /// <summary>
        /// Replaces the data in the existing attached dataset with the contents of the file
        /// </summary>
        /// <param name="filename">The location of the file to add to the dataset</param>
        /// <param name="skipHeaders">Whether to skip the first row of your import file</param>
        /// <returns>Whether it worked</returns>
        public bool append(String filename, bool skipHeaders)
        {
            var endpointName = new StringBuilder("append");
            if (skipHeaders)
                endpointName.Append("?skip_headers=true");
            return multipartAppendOrRefresh(filename, endpointName.ToString());
        }

        /// <summary>
        /// Add a row to the dataset
        /// </summary>
        /// <param name="row">A dictionary of column/celldata values</param>
        /// <returns>Success or failure</returns>
        public bool addRow(Dictionary<string,string> row) {
            if (!attached()) {
                return false;
            }
            JObject rowJson = MapToJson(row); 

            JsonPayload response = PostRequest("/views/" + _uid + "/rows.json", 
                rowJson.ToString(Formatting.None, null));

            return responseIsClean(response);
        }

        /// <summary>
        /// Make a working copy of a published dataset, with all row data copied.
        /// </summary>
        /// <returns>Uid of the new working copy view.</returns>
        public String copy()
        {
            return this.copyWithMethod("copy");
        }

        /// <summary>
        /// Make a working copy of only the schema of a published dataset, with
        /// all row data omitted.
        /// </summary>
        /// <returns>Uid of the new working copy view.</returns>
        public String copySchema()
        {
            return this.copyWithMethod("copySchema");
        }

        private String copyWithMethod(String method)
        {
            JsonPayload response = this.PostRequest(String.Format("/views/{0}/publication?method={1}", this._uid, method), "");

            bool stillWorking = true;
            do
            {
                String result = (String)response.JsonObject["status"];
                if (result != "processing")
                {
                    stillWorking = false;
                }
                else
                {
                    response = this.PostRequest(String.Format("/views/{0}/publication?method={1}", this._uid, method), "");
                }
            } while (stillWorking);

            if (!this.responseIsClean(response))
                return null;

            return (String)response.JsonObject["id"];
        }

        /// <summary>
        /// Performs a publish operation on a working copy dataset, to
        /// commit the changes you've made to the working copy to the
        /// published copy. Note that this should be called on the working
        /// copy, not the published version.
        /// </summary>
        /// <returns>Success or failure</returns>
        public bool publish()
        {
            JsonPayload response = this.PostRequest(String.Format("/views/{0}/publication", this._uid), "");
            return responseIsClean(response);
        }

        /// <summary>
        /// Like addRow, but doesn't immediately send the data away.
        /// Instead, it stores it in a queue, which can be flushed with sendBatchRequest
        /// </summary>
        /// <param name="row">A dictionary of column/celldata values</param>
        public void delayAddRow(Dictionary<string,string> row) {
            JObject rowJson = MapToJson(row);
            string rowString = rowJson.ToString(Formatting.None, null);

            BatchRequest request = new BatchRequest("POST",
                "/views/" + _uid + "/rows.json",
                rowString);
            batchQueue.Add(request);
        }

        /// <summary>
        /// Adds a column to the dataset
        /// </summary>
        /// <param name="name">The name of the column</param>
        /// <param name="description">An optional description</param>
        /// <param name="type">The column type. See API docs vor valid types</param>
        /// <param name="width">How many pixels wide to make the column</param>
        /// <param name="hidden">If true, don't show the column in views, just store the data</param>
        /// <returns></returns>
        public bool addColumn(String name, String description, String type,
            int width, bool hidden) {
            if (!attached()) {
                return false;
            }

            _log.Debug("Creating column '" + name + "' of type '" +
                   type + "'");

            JObject column = new JObject();
            column.Add("name", name);
            column.Add("description", description);
            column.Add("dataTypeName", type);
            column.Add("hidden", hidden);
            column.Add("width", width);

            JsonPayload response = PostRequest("/views/" + _uid + "/columns.json",
                column.ToString(Formatting.None, null));
            return responseIsClean(response);
        }

        public bool addColumn(String name, String description, String type, int width) {
            return addColumn(name, description, type, width, false);
        }

        public bool addColumn(String name, String description, String type) {
            return addColumn(name, description, type, DEFAULT_COLUMN_WIDTH);
        }

        public bool addColumn(String name, String description) {
            return addColumn(name, description, DEFAULT_COLUMN_TYPE);
        }

        public bool addColumn(String name) {
            return addColumn(name, "");
        }

        /// <summary>
        /// Upload a file to the dataset for use in a cell (e.g. image)
        /// </summary>
        /// <param name="filename">Where the file is located on disk</param>
        /// <returns>A UID to use in celldata, referring to this file</returns>
        public String uploadFile(String filename) {
            if (!attached()) {
                return null;
            }
            JObject response = multipartUpload("/views/" + _uid + "/files.txt", filename);

            if (response == null) {
                _log.Error("Received a null response after file upload.");
                return null;
            }
            return (string)response["file"];
        }

        /// <summary>
        /// Deletes (irreversibly) the current dataset
        /// </summary>
        /// <returns>Success if your dataset has been vanquished</returns>
        public bool delete() {
            if (!attached()) {
                return false;
            }
            return responseIsClean(genericWebRequest("/views.json/?id=" +
                _uid + "&method=delete","","DELETE"));
        }

        /// <summary>
        /// Sets the dataset's visibility
        /// </summary>
        /// <param name="isPublic">Whether or not it should be publicly viewable</param>
        /// <returns></returns>
        public bool setPublic(bool isPublic) {
            if (!attached()) {
                return false;
            }
            string paramString = isPublic ? "public.read" : "private";
            return responseIsClean(genericWebRequest("/views/" + _uid +
                "?method=setPermission&value=" + paramString, "", "PUT"));
        }

        /// <summary>
        /// Gets the metadata associated with the dataset
        /// </summary>
        /// <returns></returns>
        public JObject metadata() {
            if (!attached()) {
                return null;
            }

            JsonPayload response = GetRequest("/views/" + _uid + ".json");
            return response.JsonObject;
        }

        /// <summary>
        /// Gets a JSON representation of the columns
        /// </summary>
        /// <returns></returns>
        public JArray columns() {
            if (!attached()) {
                return null;
            }
            JsonPayload response = GetRequest("/views/" + _uid + "/columns.json");
            if (responseIsClean(response)) {
                return response.JsonArray;
            }
            return null;
        }

        /// <summary>
        /// Gets a JSON Array of the rows in the dataset
        /// </summary>
        /// <returns>The rows</returns>
        public JArray rows() {
            if (!attached()) {
                return null;
            }
            JsonPayload response = GetRequest("/views/" + _uid + "/rows.json");
            if (responseIsClean(response)) {
                return response.JsonArray;
            }
            return null;
        }

        /// <summary>
        /// Sets the dataset's attribution, e.g. "provided by"
        /// </summary>
        /// <param name="attribution">Name of the data source/organization</param>
        /// <param name="url">An optional link to the data source</param>
        public void setAttribution(String attribution, String url) {
            JObject data = new JObject();
            data.Add("attribution", attribution);
            data.Add("attributionLink", url);
            putRequest(data.ToString(Formatting.None, null));
        }

        /// <summary>
        /// Sets the dataset's description
        /// </summary>
        /// <param name="description">The description</param>
        public void setDescription(String description) {
            JObject data = new JObject();
            data.Add("description", description);
            putRequest(data.ToString(Formatting.None, null));
        }

        /// <summary>
        /// Gets an href to the dataset, shortened.
        /// </summary>
        /// <returns>The URL to view the dataset at</returns>
        public String shortUrl() {
            return httpBase + "/d/" + _uid;
        }

        private void putRequest(String body) {
            if (!attached()) {
                return;
            }
            JsonPayload response = genericWebRequest("/views/" + _uid, body, "PUT");
            if (!responseIsClean(response)) {
                _log.Error("Error in put request. See logs.");
            }
        }

        private bool multipartAppendOrRefresh(String filename, String method) {
            if (!attached()) {
                return false;
            }
            JObject response = multipartUpload("/views/" + _uid + "/rows?method=" + method, filename);

            if (response == null) {
                _log.Error("Received null response after file " + method + ".");
                return false;
            }
            return true;
        }

        /// <summary>
        /// Multipart upload a file
        /// </summary>
        /// <param name="url">Where to upload the file</param>
        /// <param name="filename">The location of the file on disk</param>
        /// <returns></returns>
        private JObject multipartUpload(String url, String filename) {
            JsonPayload response = UploadFile(url, filename);
            if (responseIsClean(response)) {
                _log.Debug("Successfully posted multipart upload.");
                return response.JsonObject;
            }
            else {
                _log.Error("Error in multipart upload.");
                return null;
            }
        }

        /// <summary>
        /// Attach to a given UID for a dataset that already exists
        /// </summary>
        /// <param name="id">The Socrata UID</param>
        public void attach(string id) {
            if (isValidId(id)) {
                _uid = id;
            }
            else {
                _log.Error("Could not use existing UID '" + id + "'. Improper format.");
            }
        }

        /// <summary>
        /// Checks if the given string represents a valid 4-4 Socrata UID
        /// </summary>
        /// <param name="id">The string to check</param>
        /// <returns>Whether or not it matches</returns>
        private static bool isValidId(String id) {
            return id != null && UID_PATTERN.IsMatch(id);
        }

        /// <summary>
        /// Check to see if the dataset is associated with a valid UID.
        /// </summary>
        /// <returns>True if association is valid</returns>
        public bool attached() {
            return isValidId(this._uid);
        }

    }
}
