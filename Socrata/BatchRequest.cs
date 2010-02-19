using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Socrata {
    public class BatchRequest {
        private JObject _data;

        public BatchRequest(string requestType, string url, string body) {
            _data = new JObject();
            _data.Add("url", url);
            _data.Add("requestType", requestType);
            _data.Add("body", body);
        }

        /// <summary>
        /// Gets the associated data
        /// </summary>
        /// <returns>The JSON representation of the batch request</returns>
        public JObject data() {
            return _data;
        }
    }
}
