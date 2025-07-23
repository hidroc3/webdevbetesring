import { Test, TestingModule } from '@nestjs/testing';
import { ArrLogManualsService } from './arr-log-manuals.service';

describe('ArrLogManualsService', () => {
  let service: ArrLogManualsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ArrLogManualsService],
    }).compile();

    service = module.get<ArrLogManualsService>(ArrLogManualsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
