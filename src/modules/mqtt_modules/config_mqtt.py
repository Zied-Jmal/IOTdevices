class ConfigMQTT:
    def __init__(self, db, collection_name):
        # self.config_json = config_json
        # self.config_data = self.config_json.load()
        self.collection_name = collection_name
        self.collection = db[collection_name]
        self.config_data = self.load()

    def load(self):
        # Load the config from MongoDB
        config = self.collection.find_one({})
        if not config:
            config = self.create_template()
            self.save(config)
        return config

    def save(self, config):
        # Save the config to MongoDB
        self.collection.replace_one({}, config, upsert=True)

    def create_template(self):
        template = {
            "broker_address": "",
            "port": 1883,  # Default MQTT port
            "client_id": "",
            "topics": [],
            "settings": {
                "keep_alive": 60,
                "qos": 0,  # Quality of Service (0: At most once, 1: At least once, 2: Exactly once)
                "retain": False,
                "username": None,
                "password": None,
                "ssl": False,  # Flag to enable SSL/TLS
                "ca_certs": None,  # Path to CA certificates file
                "certfile": None,  # Path to client certificate file
                "keyfile": None,  # Path to client private key file
                "insecure": False,  # Flag to allow insecure SSL/TLS connection
                "auth": False,  # Flag to enable authentication
            },
        }
        return template

    def reload(self):
        print("Reloading config")
        self.config_data = self.load()

    def get_value(self, key):
        self.reload()
        return self.config_data.get(key)

    def insert_value(self, key, value):
        self.reload()
        if isinstance(self.config_data.get(key), list):
            self.config_data[key].extend(value if isinstance(value, list) else [value])
            self.config_data[key] = list(
                set(self.config_data[key])
            )  # Remove duplicates
        else:
            self.config_data[key] = value
        self.save(self.config_data)

    def update_value(self, key, value):
        self.reload()
        self.config_data[key] = value
        self.save(self.config_data)

    def delete_value(self, key, value=None):
        self.reload()
        if value is None:
            if key in self.config_data:
                del self.config_data[key]
        elif isinstance(self.config_data.get(key), list):
            self.config_data[key] = [v for v in self.config_data[key] if v != value]
        self.save(self.config_data)

    def get_nested_value(self, *keys):
        self.reload()
        value = self.config_data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def insert_nested_value(self, *keys, value):
        self.reload()
        nested_dict = self.config_data
        for key in keys[:-1]:
            if isinstance(nested_dict.get(key), dict):
                nested_dict = nested_dict[key]
            else:
                nested_dict[key] = {}
                nested_dict = nested_dict[key]

        nested_dict[keys[-1]] = value
        self.save(self.config_data)

    """def create_template(self):
        self.config_data = self.config_json.load()
        template = template = {
            "broker_address": "",
            "port": 1883,  # Default MQTT port
            "client_id": "",
            "topics": [],
            "settings": {
                "keep_alive": 60,
                "qos": 0,  # Quality of Service (0: At most once, 1: At least once, 2: Exactly once)
                "retain": False,
                "username": None,
                "password": None,
                "ssl": False,  # Flag to enable SSL/TLS
                "ca_certs": None,  # Path to CA certificates file
                "certfile": None,  # Path to client certificate file
                "keyfile": None,  # Path to client private key file
                "insecure": False,  # Flag to allow insecure SSL/TLS connection
                "auth": False,  # Flag to enable authentication
            },
        }
        self.config_json.save(template)
        print("Created template config file")

    def find_type(self, template, keys):
        nested_template = template
        for key in keys[:-1]:
            nested_template = nested_template.get(key, {})
        return type(nested_template.get(keys[-1], str))

    def reload(self):
        self.config_data = self.config_json.load()

    def get_value(self, key):
        self.reload()
        return self.config_data.get(key)

    def insert_value(self, key, value):
        self.reload()
        if isinstance(self.config_data.get(key), list):
            for one_value in value:
                self.config_data[key].append(one_value)
                print(self.config_data[key])
                self.config_data[key] = list(
                    set(self.config_data[key])
                )  # Remove duplicates
        else:
            self.config_data[key] = value
        self.config_json.save(self.config_data)

    def update_value(self, key, value):
        self.reload()
        self.config_data[key] = value
        self.config_json.save(self.config_data)

    def delete_value(self, key, value=None):
        self.reload()
        if value is None:
            if key in self.config_data:
                del self.config_data[key]
        elif isinstance(self.config_data.get(key), list):
            self.config_data[key] = [v for v in self.config_data[key] if v != value]
        self.config_json.save(self.config_data)

    def get_nested_value(self, *keys):
        self.reload()
        value = self.config_data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def insert_nested_value(self, *keys, value):
        self.reload()
        nested_dict = self.config_data
        for key in keys[:-1]:
            if isinstance(nested_dict.get(key), dict):
                nested_dict = nested_dict[key]
            else:
                nested_dict[key] = {}
                nested_dict = nested_dict[key]

        # print(type(template)(value))
        nested_dict[keys[-1]] = value
        self.config_json.save(self.config_data)"""


"""
testConfig = ConfigMQTT(ConfigJson("test_config.json"))
testConfig.create_template()
testConfig.insert_value("broker_address", "mqdtt.eclipseprojects.io")
print(testConfig.get_value("broker_address"))
print(testConfig.get_value("topics"))

testConfig.insert_value("topics", "mqdtt.eclipseprojects.ios")
testConfig.insert_nested_value("settings", "keep_alive", value="10")
print(testConfig.get_nested_value("settings", "keep_alive"))
"""


""" def get_one_string_variable_by_name(self, name):
        return self.config_data.get(name, "")

    def get_one_int_variable_by_name(self, name):
        return self.config_data.get(name, None)

    def get_list_by_name(self, name):
        return self.config_data.get(name, [])

    def insert_list_by_name(self, name, List):
        list_by_name = self.get_list_by_name(name)
        list_by_name.append(List)
        list_by_name_without_duplication = list(set(list_by_name))
        self.config_data["name"] = list_by_name_without_duplication
        self.config_json.save(self.config_data)

    def remove_list_by_name(self, topic):
        topics = self.get_topics()
        topics = [t for t in topics if t != topic]
        self.config_data["topics"] = topics
        self.config_json.save(self.config_data)"""

"""class template_type_config(BaseModel):
            payment: str
            customer_id: int
            cash_flow: str
            # you'd need a custom validator to handle capitalization
            name: str
            city: str
            broker_address: str
            port: int  # Default MQTT port
            client_id: str
            topics: list
            settings: dict
            keep_alive: int
            qos: int  # Quality of Service (0: At most once 1: At least once 2: Exactly once)
            retain: bool
            username: str
            password: str
            ssl: bool  # Flag to enable SSL/TLS
            ca_certs: str  # Path to CA certificates file
            certfile: str  # Path to client certificate file
            keyfile: str  # Path to client private key file
            Insecure: str  # Flag to allow insecure SSL/TLS connection
            auth: str"""
