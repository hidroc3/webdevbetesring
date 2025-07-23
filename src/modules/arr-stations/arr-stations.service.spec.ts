import { Test, TestingModule } from '@nestjs/testing';
import { ArrStationsService } from './arr-stations.service';

describe('ArrStationsService', () => {
  let service: ArrStationsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ArrStationsService],
    }).compile();

    service = module.get<ArrStationsService>(ArrStationsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
