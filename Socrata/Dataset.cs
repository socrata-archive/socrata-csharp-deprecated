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

namespace Socrata {
    /// <summary>
    /// A Socrata dataset and associated rows/columns/metadata
    /// </summary>
    public class Dataset : ApiBase {
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

            return true;
        }

    }
}
