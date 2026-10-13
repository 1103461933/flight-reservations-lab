FROM alpine:3.19

RUN apk add --no-cache \
    jq \
    curl \
    bash \
    coreutils

COPY validator.sh /validate.sh

RUN chmod 755 /validate.sh

ENTRYPOINT ["/validate.sh"]