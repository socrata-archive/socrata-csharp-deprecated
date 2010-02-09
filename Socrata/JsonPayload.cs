using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using log4net;

namespace Socrata {
    /// <summary>
    /// Contains either a JSON Object, a JSON array, or a string on error.
    /// </summary>
    public class JsonPayload {
        private JObject _jsonObject;
        private JArray  _jsonArray;
        private String  _message;

        public JArray   JsonArray     { get { return _jsonArray; } }
        public JObject  JsonObject    { get { return _jsonObject; } }
        public String   Message       { get { return _message; } }

        public JsonPayload(String payload) {
            parseString(payload);
        }

        private void parseString(String payload) {
            try {
                _jsonObject = JObject.Parse(payload);
            }
            catch (Exception) {
                try {
                    _jsonArray = JArray.Parse(payload);
                }
                catch (Exception exor) {
                    LogManager.GetLogger(typeof(JsonPayload)).Warn("Failed to detect JSON Array or Object.", exor);
                    _message = payload;
                }
            }
        }
    }
}
