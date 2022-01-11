# docker build -t ipaloma/tornado/app:V1.00 .
# docker run -d -p 80:80 --name web --mount source=docker_volume_USB,target=/torndo_web/log --mount source=rds_porting,target=/mnt ipaloma/tornado/app:V1.00
# 从ubuntu版本21.10开始生成，用香港节点来生成，可以避免访问外网资源失败
FROM ubuntu:21.10

RUN apt-get install -y locales 
RUN rm -rf /var/lib/apt/lists/* 
RUN localedef -i zh_CN -c -f UTF-8 -A /usr/share/locale/locale.alias zh_CN.UTF-8

ENV LANG=zh_CN.UTF-8

RUN apt-get update 

RUN apt-get -y -q install curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o /get-pip.py
RUN apt-get -y -q install python3-distutils
RUN python3 /get-pip.py
RUN python3 -m pip install pipreqs
RUN apt -y -q autoremove
#CMD ["bash"]
# 上述步骤可以生成一个单独image缓存到镜像仓库
# FROM registry.cn-hangzhou.aliyuncs.com/ipaloma/ubuntu.python:V1.00
RUN echo "Asia/Shanghai" > /etc/timezone
ENV PATH=${PATH}:/tornado_web
ENV TZ=Asia/Shanghai
ENV incloud=1
ENV PYTHONIOENCODING=utf-8
EXPOSE 80/tcp
WORKDIR /tornado_web
COPY . .
# 从代码查找依赖关系 ? 后续还是要考虑在开发环境生成，避免版本不一致
RUN pipreqs ./  --encoding=utf8  --force
# RUN python3 -m pip freeze > requirements.txt
RUN python3 -m pip install -r requirements.txt

# for development
# RUN apt-get -y -q install git   

ENTRYPOINT [ "python3" ]
CMD ["main.py"]
