from abc import ABC, abstractmethod
import random
import uuid
from datetime import datetime
from .UserManager import UserManager
from confluent_kafka.serialization import SerializationContext

user_manager = UserManager()  # Shared instance across all events


class Event(ABC):
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


class MovieCatalogEvent(Event):
    """Handles movie catalog enrichment events"""

    def __init__(self, movie_id, title, genre, list_price):
        super().__init__("movie_catalog_enriched")
        self.movie_id = str(movie_id)
        self.title = title if title else "Unknown"
        self.genre = genre if genre else "Unknown"
        self.list_price = float(list_price) if list_price else 0.0

    @staticmethod
    def to_dict(obj, ctx: SerializationContext = None):
        """Returns event data as a dictionary"""
        return {
            "timestamp": obj.timestamp,
            "event_name": obj.event_name,
            "movie_id": obj.movie_id,
            "title": obj.title,
            "genre": obj.genre,
            "list_price": obj.list_price
        }

    def __call__(self, ctx: SerializationContext = None):
        """Make the instance callable to return its dictionary representation"""
        return self.to_dict(self, ctx)


class UserRegistrationEvent(Event):
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


class SignInEvent(Event):
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


class SignOutEvent(Event):
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


class ItemViewEvent(Event):
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


class AddToCartEvent(Event):
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


class CheckoutEvent(Event):
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