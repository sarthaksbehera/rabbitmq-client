# UBI-based Python image is OpenShift-friendly
FROM registry.access.redhat.com/ubi9/python-311
WORKDIR /opt/app-root/src
# Copy dependency files first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy app
COPY . .
# OpenShift runs with random UID; ensure group perms allow writes if needed
# (Often not required unless you write to the filesystem)
RUN chmod -R g=u /opt/app-root/src
# No fixed USER — OpenShift will set an arbitrary UID
CMD ["python", "main.py"]
