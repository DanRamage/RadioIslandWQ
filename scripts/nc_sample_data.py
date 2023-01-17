import sys
sys.path.append('../../commonfiles/python')

from wq_output_results import wq_sample_data

class nc_wq_sample_data(wq_sample_data):
  def __init__(self, **kwargs):
    wq_sample_data.__init__(self, **kwargs)
    self._site_id = kwargs.get('site_id', None)
    self._entero_gm = kwargs.get('entero_gm', None)
    self._entero_ssm = kwargs.get('entero_ssm', None)
    self._entero_gm2 = kwargs.get('entero_gm2', None)
    self._entero_ssm_cfu = kwargs.get('entero_ssm_cfu', None)

  @property
  def site_id(self):
    return self._site_id
  @site_id.setter
  def site_id(self, value):
    self._site_id = value

  @property
  def entero_gm(self):
    return self._entero_gm
  @entero_gm.setter
  def entero_gm(self, value):
    self._entero_gm = value

  @property
  def entero_gm2(self):
    return self._entero_gm2
  @entero_gm2.setter
  def entero_gm2(self, value):
    self._entero_gm2 = value

  @property
  def entero_ssm(self):
    return self._entero_ssm
  @entero_ssm.setter
  def entero_ssm(self, value):
    self._entero_ssm = value

  @property
  def entero_ssm_cfu(self):
    return self._entero_ssm_cfu

  @entero_ssm_cfu.setter
  def entero_ssm_cfu(self, value):
    self._entero_ssm_cfu = value

  @property
  def value(self):
    return self._value

  @value.setter
  def value(self, value):
    self._value = value
