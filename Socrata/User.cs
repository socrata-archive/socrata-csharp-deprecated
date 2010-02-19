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
using System.Text;
using Newtonsoft.Json.Linq;

namespace Socrata {
    /// <summary>
    /// Represents a Socrata user
    /// </summary>
    public class User : ApiBase {
        private string _username;

        public User(String username) : base() {
            this._username = username;
        }

        /// <summary>
        /// Gets all the publicly accessible datasets belonging to this user
        /// </summary>
        /// <returns>A list of datasets</returns>
        public List<Dataset> datasets() {
            JsonPayload response = GetRequest("/users/" + _username + "/views.json");
            if (!responseIsClean(response)) {
                _log.Error("Could not get datasets belonging to '" + _username + "'");
                return null;
            }
            JArray sets = response.JsonArray;
            List<Dataset> results = new List<Dataset>();

            for (int i = 0; i < sets.Count; i++) {
                Dataset set = new Dataset();
                JObject setObject = (JObject) sets[i];
                string setUID = (string) setObject["id"];
                set.attach(setUID);
                results.Add(set);
            }
            return results;
        }


    }

}
