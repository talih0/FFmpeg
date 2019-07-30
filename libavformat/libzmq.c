/*
 * ZMQ URLProtocol
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <zmq.h>
#include "url.h"
#include "network.h"
#include "libavutil/avstring.h"
#include "libavutil/opt.h"

typedef struct ZMQContext {
    const AVClass *class;
    void *context;
    void *socket;
    unsigned int timeout; /*blocking timeout during zmq poll in milliseconds */
} ZMQContext;

static const AVOption options[] = {
    { NULL }
};

static int ff_zmq_open(URLContext *h, const char *uri, int flags)
{
    int ret;
    ZMQContext *s   = h->priv_data;
    s->context      = zmq_ctx_new();
    h->is_streamed  = 1;

    av_strstart(uri, "zmq:", &uri);

    /*publish during write*/
    if (h->flags & AVIO_FLAG_WRITE) {
        s->socket = zmq_socket(s->context, ZMQ_PUB);
        if (!s->socket) {
            av_log(h, AV_LOG_ERROR, "Error occured during zmq_socket(): %s\n", zmq_strerror(errno));
            zmq_ctx_destroy(s->context);
            return AVERROR_EXTERNAL;
        }

        ret = zmq_bind(s->socket, uri);
        if (ret < 0) {
            av_log(h, AV_LOG_ERROR, "Error occured during zmq_bind(): %s\n", zmq_strerror(errno));
            zmq_close(s->socket);
            zmq_ctx_destroy(s->context);
            return AVERROR_EXTERNAL;
        }
    }

    /*subscribe for read*/
    if (h->flags & AVIO_FLAG_READ) {
        s->socket = zmq_socket(s->context, ZMQ_SUB);
        if (!s->socket) {
            av_log(h, AV_LOG_ERROR, "Error occured during zmq_socket(): %s\n", zmq_strerror(errno));
            zmq_ctx_destroy(s->context);
            return AVERROR_EXTERNAL;
        }

        zmq_setsockopt(s->socket, ZMQ_SUBSCRIBE, "", 0);
        ret = zmq_connect(s->socket, uri);
        if (ret == -1) {
            av_log(h, AV_LOG_ERROR, "Error occured during zmq_connect(): %s\n", zmq_strerror(errno));
            zmq_close(s->socket);
            zmq_ctx_destroy(s->context);
            return AVERROR_EXTERNAL;
        }
    }
    return 0;
}

static int ff_zmq_write(URLContext *h, const unsigned char *buf, int size)
{
    int ret;
    ZMQContext *s = h->priv_data;

    ret = zmq_send(s->socket, buf, size, ZMQ_DONTWAIT);
    if (ret >= 0)
        return ret; /*number of sent bytes*/

    /*errno = EAGAIN if messages cannot be pushed*/
    if (ret == -1 && errno == EAGAIN) {
        return AVERROR(EAGAIN);
    } else
        return AVERROR_EXTERNAL;
}

static int ff_zmq_read(URLContext *h, unsigned char *buf, int size)
{
    int ret;
    ZMQContext *s = h->priv_data;
    zmq_msg_t msg;
    int msg_size;

    ret = zmq_msg_init(&msg);
    if (ret == -1) {
      av_log(h, AV_LOG_ERROR, "Error occured during zmq_msg_init(): %s\n", zmq_strerror(errno));
      return AVERROR_EXTERNAL;
    }

    ret = zmq_msg_recv(&msg, s->socket, ZMQ_DONTWAIT);
    if (ret == -1) {
        ret = (errno == EAGAIN) ? AVERROR(EAGAIN) : AVERROR_EXTERNAL;
        if (ret == AVERROR_EXTERNAL)
          av_log(h, AV_LOG_ERROR, "Error occured during zmq_msg_recv(): %s\n", zmq_strerror(errno));
        goto finish;
    }

    msg_size = zmq_msg_size(&msg);
    if (msg_size > size) {
        msg_size = size;
        av_log(h, AV_LOG_WARNING, "ZMQ message exceeds available space in the buffer. Message will be truncated\n");
    }
    memcpy(buf, zmq_msg_data(&msg), msg_size);

finish:
    zmq_msg_close(&msg);
    return ret;
}

static int ff_zmq_close(URLContext *h)
{
    ZMQContext *s = h->priv_data;
    zmq_close(s->socket);
    zmq_ctx_destroy(s->context);
    return 0;
}

static const AVClass zmq_context_class = {
    .class_name = "zmq",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};

const URLProtocol ff_libzmq_protocol = {
    .name            = "zmq",
    .url_close       = ff_zmq_close,
    .url_open        = ff_zmq_open,
    .url_read        = ff_zmq_read,
    .url_write       = ff_zmq_write,
    .priv_data_size  = sizeof(ZMQContext),
    .priv_data_class = &zmq_context_class,
    .flags           = URL_PROTOCOL_FLAG_NETWORK,
};
