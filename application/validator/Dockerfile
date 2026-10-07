FROM alpine:3.19

RUN apk add --no-cache jq curl bash

COPY validate.sh /validate.sh
RUN chmod +x /validate.sh

ENTRYPOINT ["/validate.sh"]