using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Socrata {
    /// <summary>
    /// Contains either a JSON Object, a JSON array, or a string on error.
    /// </summary>
    class JsonPayload {
        private JObject jsonObject;
        private JArray jsonArray;
        private String message;

        public JsonPayload(String payload) {

        }
    }
}
