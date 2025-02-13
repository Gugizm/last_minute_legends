import random

class UserManager:
    """Manages unique users for event simulation"""

    def __init__(self):
        self.existing_users = set()
        self.email_providers = ["gmail.com", "yahoo.com", "outlook.com"]

    def generate_unique_user_id(self):
        """Generate a unique user ID (1 - 1,000,000)"""
        while True:
            user_id = random.randint(1, 1000000)
            if user_id not in self.existing_users:
                self.existing_users.add(user_id)
                return user_id

    def get_existing_user(self):
        """Return an existing user ID if available, otherwise generate a new one"""
        if self.existing_users:
            return random.choice(list(self.existing_users))
        return self.generate_unique_user_id()

    def generate_masked_email(self, user_id):
        """Generate a masked email format"""
        username = f"user{user_id}"
        provider = random.choice(self.email_providers)
        return f"{username}@{provider}"
