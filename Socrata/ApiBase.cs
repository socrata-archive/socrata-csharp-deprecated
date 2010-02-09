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
using log4net;
using log4net.Config;
using System.IO;
using System.Configuration;

namespace Socrata {
    /// <summary>
    /// Base class under which all Socrata classes inherit.
    /// </summary>
    public class ApiBase {
        protected static ILog _log;
        protected NetworkCredential credentials;
        protected string httpBase;

        public ApiBase() {
            _log = LogManager.GetLogger(typeof(ApiBase));
            // Sets up log4net to use a root level logger with ConsoleAppend
            BasicConfigurator.Configure();

            credentials = new NetworkCredential(ConfigurationSettings.AppSettings["socrata.username"],
                ConfigurationSettings.AppSettings["socrata.password"]);
            httpBase = ConfigurationSettings.AppSettings["socrata.host"];
        }

        /// <summary>
        /// Performs a generic GET request on the API server
        /// </summary>
        /// <param name="url">The URL to request from</param>
        /// <returns>The JSON response</returns>
        protected JsonPayload GetRequest(String url) {
            HttpWebRequest request = (HttpWebRequest) WebRequest.Create(httpBase + url);
            HttpWebResponse response;
            try {
                response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException ex) {
                _log.Error("Could not get response from GET request.", ex);
                return null;
            }

            Stream responseStream = response.GetResponseStream();
            StringBuilder sb = new StringBuilder();

            string temp = null;
            int count = 0;
            byte[] buffer = new byte[8192];

            // Read to the end of the response
            do {
                count = responseStream.Read(buffer, 0, buffer.Length);

                if (count != 0) {
                    temp = Encoding.ASCII.GetString(buffer, 0, count);
                    sb.Append(temp);
                }
            } while (count > 0);

            return new JsonPayload(sb.ToString());
        }

        /// <summary>
        /// Performs a generice POST request on the API server
        /// </summary>
        /// <param name="url">Where to send the post request</param>
        /// <param name="parameters">The data to accompany the post request</param>
        /// <returns>The JSON response</returns>
        protected JsonPayload PostRequest(String url, String parameters) {
            WebRequest request = WebRequest.Create(httpBase + url);
            request.Credentials = credentials;

            request.ContentType = "application/x-www-form-urlencoded";
            request.Method = "POST";

            byte[] bytes = Encoding.ASCII.GetBytes(parameters);
            Stream outputStream = null;
            try {
                request.ContentLength = bytes.Length;
                outputStream = request.GetRequestStream();
                outputStream.Write(bytes, 0, bytes.Length);
            }
            catch (WebException ex) {
                _log.Error("Error sending data to server.", ex);
                return null;
            }
            finally {
                if (outputStream != null) {
                    outputStream.Close();
                }
            }

            try {
                WebResponse response = request.GetResponse();
                if (response == null) {
                    return null;
                }

                StreamReader reader = new StreamReader(response.GetResponseStream());
                return new JsonPayload(reader.ReadToEnd());
            }
            catch (WebException ex) {
                _log.Error("Error receiving response from server.", ex);
                return null;
            }
        }

    }
}
