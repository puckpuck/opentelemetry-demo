# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

FROM python:3.12-alpine

WORKDIR /app/test/demo-test

COPY test/demo-test/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY test/demo-test/ ./

ENTRYPOINT ["python", "runner.py"]
