from financial_analyzer.core.peer_benchmark_engine import PeerBenchmarkEngine


def test_peer_engine_smoke():
    p=PeerBenchmarkEngine()
    peers=p.get_peers('AAPL','hardware_platform',['AAPL','MSFT'])
    assert 'local' in peers

