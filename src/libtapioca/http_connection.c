/*
    Copyright (C) 2013 University of Lugano

	This file is part of the MoSQL storage system. 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "http_connection.h"
#include <stdlib.h>


static void 
http_request_done(struct evhttp_request *r, void *arg) {
	http_connection* hc;
	hc = (http_connection*)arg;
	hc->response_code = r->response_code;
	if (hc->b != NULL) {
		evbuffer_drain(hc->b, EVBUFFER_LENGTH(hc->b));
		evbuffer_add_buffer(hc->b, r->input_buffer);
	}
	event_base_loopexit(hc->base, NULL);
}


http_connection* 
http_connection_new(char* address, int port) {
	http_connection* hc;
	hc = malloc(sizeof(http_connection));
	if (hc == NULL) return NULL;
	hc->base = event_base_new();
	if (hc->base == NULL) return NULL;
	hc->c = evhttp_connection_new(address, port);
	evhttp_connection_set_base(hc->c, hc->base);
	return hc;
}


void
http_connection_free(http_connection* hc) {
	event_base_free(hc->base);
	evhttp_connection_free(hc->c);
	free(hc);
}


int
http_connection_request(http_connection* hc, char* uri, struct evbuffer* body) {
	struct evhttp_request* r;
	hc->b = body;
	r = evhttp_request_new(http_request_done, hc);
	evhttp_add_header(r->output_headers, "Keep-Alive", "10");
	evhttp_make_request(hc->c, r, EVHTTP_REQ_GET, uri);
	event_base_dispatch(hc->base);
	return hc->response_code;
}


int
http_connection_post_request(http_connection* hc, char* uri, struct evbuffer* post, struct evbuffer* body) {
	struct evhttp_request* r;
	hc->b = body;
	r = evhttp_request_new(http_request_done, hc);
	evhttp_add_header(r->output_headers, "Keep-Alive", "10");
	evbuffer_add_buffer(r->output_buffer, post);
	evhttp_make_request(hc->c, r, EVHTTP_REQ_POST, uri);
	event_base_dispatch(hc->base);
	return hc->response_code;
}
