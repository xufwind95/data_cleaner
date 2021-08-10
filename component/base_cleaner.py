#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: base_cleaner
:Author: xufeng@bbdservice.com 
:Date: 2021-08-06 6:39 PM
:Version: v.1.0
:Description:
"""
import abc


class BaseCleaner(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def clean(self):
        raise NotImplementedError

    def test_clean(self):
        self.test = True
        self.clean()

    def set_action_log_prefix(self, prefix: str = '====='):
        self.action_prefix = prefix

    @property
    def description(self) -> str:
        raise NotImplementedError

    @property
    def test(self):
        if hasattr(self, '_test'):
            return self._test
        return False

    @test.setter
    def test(self, test_exec=True):
        setattr(self, '_test', test_exec)

    @property
    def action_prefix(self):
        if hasattr(self, '_action_prefix'):
            return self._action_prefix
        return '====='

    @action_prefix.setter
    def action_prefix(self, prefix='====='):
        setattr(self, '_action_prefix', prefix)

    def __repr__(self):
        return self.description
