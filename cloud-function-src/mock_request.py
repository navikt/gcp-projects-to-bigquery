# mock_request.py
from main import main


class MockRequest:
    def __init__(self, data):
        self.data = data

    def get_json(self):
        return self.data

if __name__ == "__main__":
    mock_data = {}  # Add any necessary mock data here
    request = MockRequest(mock_data)
    response = main(request)
    print(response) 