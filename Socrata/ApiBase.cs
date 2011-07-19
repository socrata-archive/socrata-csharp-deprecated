/*

Copyright (c) 2011 Socrata.

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
        protected static ILog           _log;
        protected NetworkCredential     credentials;
        protected string                httpBase;
        protected string                appToken;
        protected List<BatchRequest>    batchQueue;

        public ApiBase() {
            batchQueue = new List<BatchRequest>();
            _log = LogManager.GetLogger(typeof(ApiBase));
            // Sets up log4net to use a root level logger with ConsoleAppend
            BasicConfigurator.Configure();

            credentials = new NetworkCredential(ConfigurationManager.AppSettings["socrata.username"],
                ConfigurationManager.AppSettings["socrata.password"]);
            httpBase = ConfigurationManager.AppSettings["socrata.host"];
            appToken = ConfigurationManager.AppSettings["socrata.app_token"];
        }

        /// <summary>
        /// Performs a generic GET request on the API server
        /// </summary>
        /// <param name="url">The URL to request from</param>
        /// <returns>The JSON response</returns>
        protected JsonPayload GetRequest(String url) {
            HttpWebRequest request  = (HttpWebRequest) WebRequest.Create(httpBase + url);
            request.PreAuthenticate = true;
            request.Credentials     = credentials;

            request.Headers.Add("X-App-Token", appToken);
            HttpWebResponse response;
            try {
                response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException ex) {
                _log.Error("Could not get response from GET request.", ex);
                return null;
            }

            Stream responseStream = response.GetResponseStream();
            StringBuilder sb      = new StringBuilder();

            string temp   = null;
            int count     = 0;
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

        protected string getAuthorization() {
            string creds = String.Format("{0}:{1}", credentials.UserName, credentials.Password);
            byte[] bytes = Encoding.ASCII.GetBytes(creds);
            string base64 = Convert.ToBase64String(bytes);
            return "Basic " + base64;
        }

        protected JsonPayload genericWebRequest(String url, String parameters,
            String method) {
            HttpWebRequest request  = (HttpWebRequest)WebRequest.Create(httpBase + url);
            request.PreAuthenticate = true;
            request.Credentials     = credentials;
            request.Method          = method;
            request.Headers.Add("X-App-Token", appToken);
            request.Headers.Add("Authorization", getAuthorization());

            bytes = Encoding.ASCII.GetBytes(parameters);

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
                String read = reader.ReadToEnd();

                return new JsonPayload(read);
            }
            catch (WebException ex) {
                _log.Error("Error receiving response from server.", ex);
                return null;
            }
        }

        /// <summary>
        /// Performs a generice POST request on the API server
        /// </summary>
        /// <param name="url">Where to send the post request</param>
        /// <param name="parameters">The data to accompany the post request</param>
        /// <returns>The JSON response</returns>
        protected JsonPayload PostRequest(String url, String parameters) {
            return genericWebRequest(url, parameters, "POST");
        }

        /// <summary>
        /// For uploading a file and returning JSON response.
        /// </summary>
        /// <param name="url">Where to upload the file</param>
        /// <param name="file">The file location on disk</param>
        /// <returns></returns>
        protected JsonPayload UploadFile(String url, String file) {
            WebClient webClient   = new WebClient();
            webClient.Credentials = credentials;
            webClient.Headers.Add("X-App-Token", appToken);
            webClient.Headers.Add("Authorization", getAuthorization());

            byte[] responseArray;
            try {
                responseArray = webClient.UploadFile(httpBase + url, file);
                String responseString = System.Text.Encoding.ASCII.GetString(responseArray);
                return new JsonPayload(responseString);
            }
            catch (WebException ex) {
                _log.Error("Error uploading file.", ex);
                return null;
            }
        }

        /// <summary>
        /// Processess any batch requests saved up in the queue
        /// </summary>
        public void sendBatchRequest() {
            if (batchQueue.Count < 1) {
                _log.Info("Call to sendBatchRequest ignored because batchQueue is empty");
                return;
            }
            JArray batches = new JArray();
            foreach (BatchRequest b in batchQueue) {
                batches.Add(b.data());
            }

            JObject bodyObject = new JObject();
            bodyObject.Add("requests", batches);

            JsonPayload response = genericWebRequest("/batches", bodyObject.ToString(Formatting.None, null), "POST");
            if (responseIsClean(response)) {
                if (response.JsonArray.Count != batchQueue.Count)
                {
                    int lastRow = response.JsonArray.Count-1;
                    _log.Error("Error in request", new Exception(response.JsonArray[lastRow].ToString()));
                    batchQueue.RemoveRange(0, lastRow+1);
                    sendBatchRequest();
                }
                _log.Debug("Sent batch requests: " + batchQueue.Count + " total.");
                batchQueue.Clear();
            }
            else {
                _log.Error("Error sending batch request.");
            }
        }

        /// <summary>
        /// Checks response object to see if any errors are present
        /// </summary>
        /// <param name="response">The JSON response returned from the server</param>
        /// <returns>True if no errors, false otherwise</returns>
        protected bool responseIsClean(JsonPayload response) {
            if (response == null) {
                return false;
            }

            if (response.Message != null && response.Message.Length > 0) {
                _log.Warn("Non-JSON response: " + response.Message);
                return false;
            }
            else if (response.JsonObject != null && response.JsonObject["error"] != null) {
                    _log.Error("Error in response: " + (string)response.JsonObject["error"]);
                    return false;
            }
            return true;
        }

        protected static JObject MapToJson(Dictionary<string, string> data) {
            JObject json = new JObject();
            foreach (string k in data.Keys) {
                json.Add(k, data[k]);
            }
            return json;
        }
    }
}
