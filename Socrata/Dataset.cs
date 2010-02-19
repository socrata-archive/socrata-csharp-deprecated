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

namespace Socrata {
    /// <summary>
    /// A Socrata dataset and associated rows/columns/metadata
    /// </summary>
    public class Dataset : ApiBase {
        private string uid;
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
                        this.uid = uid;
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
        /// Checks if the given string represents a valid 4-4 Socrata UID
        /// </summary>
        /// <param name="id">The string to check</param>
        /// <returns>Whether or not it matches</returns>
        private static bool isValidId(String id) {
            return UID_PATTERN.IsMatch(id);
        }

    }
}
