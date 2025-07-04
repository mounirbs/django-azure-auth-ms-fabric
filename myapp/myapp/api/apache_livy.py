"""
Apache Livy REST API client.

This module provides the ApacheLivy class, which wraps the main endpoints of the
Livy REST API (see: https://livy.apache.org/docs/latest/rest-api.html).

Usage:
    from myapp.interface.apache_livy import ApacheLivy

    livy = ApacheLivy(base_url="https://livy-server-url", access_token="...", timeout=30)
    # Create a session
    response = livy.create_session(data={...})
    # Submit a statement
    response = livy.submit_statement(session_id, code="print(1+1)")
    # Get session state
    response = livy.get_session_state(session_id)
    # And so on...

Each method returns a `requests.Response` object.

All methods accept optional `headers`, `params`, and `timeout` arguments to allow
customization of the HTTP request.

Session API:
    - create_session
    - list_sessions
    - get_session
    - delete_session
    - get_session_state
    - get_session_log

Statements API:
    - submit_statement
    - list_statements
    - get_statement
    - cancel_statement

Batches API:
    - create_batch
    - list_batches
    - get_batch
    - delete_batch
    - get_batch_state
    - get_batch_log

See each method's docstring for details.
"""
import requests

class ApacheLivy:
    """
    Apache Livy REST API client.
    See: https://livy.apache.org/docs/latest/rest-api.html
    """

    def __init__(self, base_url, access_token=None, timeout=30):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.timeout = timeout

    def _headers(self, headers=None):
        base_headers = {"Content-Type": "application/json"}
        if self.access_token:
            base_headers["Authorization"] = f"Bearer {self.access_token}"
        if headers:
            base_headers.update(headers)
        return base_headers

    # Sessions API
    def create_session(self, data, headers=None, params=None, timeout=None):
        """POST /sessions"""
        url = f"{self.base_url}/sessions"
        resp = requests.post(
            url,
            json=data,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def list_sessions(self, headers=None, params=None, timeout=None):
        """GET /sessions"""
        url = f"{self.base_url}/sessions"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_session(self, session_id, headers=None, params=None, timeout=None):
        """GET /sessions/{sessionId}"""
        url = f"{self.base_url}/sessions/{session_id}"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def delete_session(self, session_id, headers=None, params=None, timeout=None):
        """DELETE /sessions/{sessionId}"""
        url = f"{self.base_url}/sessions/{session_id}"
        resp = requests.delete(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_session_state(self, session_id, headers=None, params=None, timeout=None):
        """GET /sessions/{sessionId}/state"""
        url = f"{self.base_url}/sessions/{session_id}/state"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_session_log(self, session_id, from_line=None, size=None, headers=None, params=None, timeout=None):
        """GET /sessions/{sessionId}/log"""
        url = f"{self.base_url}/sessions/{session_id}/log"
        query = params.copy() if params else {}
        if from_line is not None:
            query["from"] = from_line
        if size is not None:
            query["size"] = size
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=query,
            timeout=timeout or self.timeout
        )
        return resp

    # Statements API
    def submit_statement(self, session_id, code, kind="pyspark", headers=None, params=None, timeout=None):
        """POST /sessions/{sessionId}/statements"""
        url = f"{self.base_url}/sessions/{session_id}/statements"
        data = {"code": code, "kind": kind}
        resp = requests.post(
            url,
            json=data,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def list_statements(self, session_id, headers=None, params=None, timeout=None):
        """GET /sessions/{sessionId}/statements"""
        url = f"{self.base_url}/sessions/{session_id}/statements"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_statement(self, session_id, statement_id, headers=None, params=None, timeout=None):
        """GET /sessions/{sessionId}/statements/{statementId}"""
        url = f"{self.base_url}/sessions/{session_id}/statements/{statement_id}"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def cancel_statement(self, session_id, statement_id, headers=None, params=None, timeout=None):
        """POST /sessions/{sessionId}/statements/{statementId}/cancel"""
        url = f"{self.base_url}/sessions/{session_id}/statements/{statement_id}/cancel"
        resp = requests.post(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    # Batches API (optional, for batch jobs)
    def create_batch(self, data, headers=None, params=None, timeout=None):
        """POST /batches"""
        url = f"{self.base_url}/batches"
        resp = requests.post(
            url,
            json=data,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def list_batches(self, headers=None, params=None, timeout=None):
        """GET /batches"""
        url = f"{self.base_url}/batches"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_batch(self, batch_id, headers=None, params=None, timeout=None):
        """GET /batches/{batchId}"""
        url = f"{self.base_url}/batches/{batch_id}"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def delete_batch(self, batch_id, headers=None, params=None, timeout=None):
        """DELETE /batches/{batchId}"""
        url = f"{self.base_url}/batches/{batch_id}"
        resp = requests.delete(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_batch_state(self, batch_id, headers=None, params=None, timeout=None):
        """GET /batches/{batchId}/state"""
        url = f"{self.base_url}/batches/{batch_id}/state"
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=params,
            timeout=timeout or self.timeout
        )
        return resp

    def get_batch_log(self, batch_id, from_line=None, size=None, headers=None, params=None, timeout=None):
        """GET /batches/{batchId}/log"""
        url = f"{self.base_url}/batches/{batch_id}/log"
        query = params.copy() if params else {}
        if from_line is not None:
            query["from"] = from_line
        if size is not None:
            query["size"] = size
        resp = requests.get(
            url,
            headers=self._headers(headers),
            params=query,
            timeout=timeout or self.timeout
        )
        return resp
