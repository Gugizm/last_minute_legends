from abc import ABC, abstractmethod
import random
import uuid
from datetime import datetime
from UserManager import UserManager
from confluent_kafka.serialization import SerializationContext

user_manager = UserManager()  # Shared instance across all events

class UserEvent(ABC):
    """Base class for all user events"""

    def __init__(self, event_name, user_id=None):
        self.timestamp = datetime.utcnow().isoformat()
        self.event_name = event_name
        self.user_id = user_id if user_id else user_manager.generate_unique_user_id()

    @staticmethod
    @abstractmethod
    def to_dict(obj, ctx: SerializationContext):
        """Method to be implemented by child classes to return event as dictionary"""
        pass

    def __call__(self, ctx: SerializationContext = None):
        """Make the instance callable to return its dictionary representation"""
        return self.to_dict(self, ctx)


class UserRegistrationEvent(UserEvent):
    """Handles user registration event"""

    def __init__(self):
        super().__init__("consumer_registration")
        self.age = random.randint(18, 95)
        self.masked_email = user_manager.generate_masked_email(self.user_id)
        self.preferred_language = random.choice(["eng", "geo", ""])

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id),
            "age": obj.age,
            "masked_email": obj.masked_email,
            "preferred_language": obj.preferred_language
        }


class SignInEvent(UserEvent):
    """Handles user sign-in event"""

    def __init__(self):
        super().__init__("sign_in", user_manager.get_existing_user())

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id)
        }


class SignOutEvent(UserEvent):
    """Handles user sign-out event"""

    def __init__(self):
        super().__init__("sign_out", user_manager.get_existing_user())

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id)
        }


class ItemViewEvent(UserEvent):
    """Handles item view event"""

    def __init__(self, movies):
        super().__init__("item_view", user_manager.get_existing_user())
        self.item_id = random.choice(movies)

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id),
            "item_id": str(obj.item_id)
        }


class AddToCartEvent(UserEvent):
    """Handles add to cart event"""

    def __init__(self, movies):
        super().__init__("added_to_cart", user_manager.get_existing_user())
        self.item_id = random.choice(movies)
        self.cart_id = str(uuid.uuid4())

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id),
            "item_id": str(obj.item_id),
            "cart_id": obj.cart_id
        }


class CheckoutEvent(UserEvent):
    """Handles checkout event"""

    def __init__(self, cart_ids):
        super().__init__("checkout_to_cart", user_manager.get_existing_user())
        self.cart_id = random.choice(cart_ids) if cart_ids else str(uuid.uuid4())
        self.payment_method = random.choice(["Cash", "Card"])

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "user_id": str(obj.user_id),
            "cart_id": obj.cart_id,
            "payment_method": obj.payment_method
        }
