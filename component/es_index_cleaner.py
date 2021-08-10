#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:File Name: es_index_cleaner
:Author: xufeng
:Date: 2021-08-06 6:47 PM
:Version: v.1.0
:Description:
"""
from component.base_cleaner import BaseCleaner


class ESIndexesCleaner(BaseCleaner):

    def __init__(self):
        pass

    @property
    def description(self) -> str:
        return "es table cleaner"

    def clean(self):
        raise NotImplementedError

