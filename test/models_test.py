# TODO TEST add  more tests

from cdmtaskservice import models  # @UnusedImport


def test_files_per_container():
    _files_per_container(1, 1, 1, [[1]])
    _files_per_container(1, 2, 1, [[1, 2]])
    _files_per_container(2, 1, 1, [[1]])
    _files_per_container(2, 2, 2, [[1], [2]])
    _files_per_container(3, 2, 2, [[1], [2]])
    _files_per_container(3, 3, 3, [[1], [2], [3]])
    _files_per_container(3, 4, 3, [[1, 2], [3], [4]])
    _files_per_container(3, 5, 3, [[1, 2], [3, 4], [5]])
    _files_per_container(3, 6, 3, [[1, 2], [3, 4], [5, 6]])
    _files_per_container(3, 7, 3, [[1, 2, 3], [4, 5], [6, 7]])


def _files_per_container(containers: int, files: int, expcont: int, expfiles: list[list[int]]):
    ji = models.JobInput(
        cluster=models.Cluster.PERLMUTTER_JAWS,
        image="fakeimage",
        params=models.Parameters(),
        num_containers=containers,
        input_files=[f"foo/bar{i}" for i in range(1, files + 1)],
        output_dir="foo/bar"
    )
    fpc = ji.get_files_per_container()
    exp = []
    for lst in expfiles:
        exp.append([f"foo/bar{i}" for i in lst])
    assert fpc == models.FilesPerContainer(expcont, exp)
    
    ji = models.JobInput(
        cluster=models.Cluster.PERLMUTTER_JAWS,
        image="fakeimage",
        params=models.Parameters(),
        num_containers=containers,
        input_files=[models.S3File(file=f"foo/bar{i}") for i in range(1, files + 1)],
        output_dir="foo/bar"
    )
    fpc = ji.get_files_per_container()
    exp = []
    for lst in expfiles:
        exp.append([models.S3File(file=f"foo/bar{i}") for i in lst])
    assert fpc == models.FilesPerContainer(expcont, exp)
