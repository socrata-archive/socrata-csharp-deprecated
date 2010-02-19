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

        public override String ToString() {
            String rep = "";
            if (_jsonObject != null) {
                rep += "Object: " + _jsonObject.ToString() + " ";
            }
            if (_jsonArray != null) {
                rep += "Array: " + _jsonArray.ToString() + " ";
            }
            if (_message != null) {
                rep += "String: " + _message;
            }
            return rep;
        }

        /// <summary>
        /// Looks at a string, converting it to a JSON Array or Object
        /// </summary>
        /// <param name="payload">The JSON string</param>
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
